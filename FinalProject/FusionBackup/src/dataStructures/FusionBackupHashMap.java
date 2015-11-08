package dataStructures;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.*;
import java.util.concurrent.Semaphore;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnMessageReceivedListener;
import constants.MessageType;
import constants.NodeType;
import constants.UpdateType;
import exceptions.DecodeException;
import exceptions.RecoverFailureException;

/**
 * Fusion-backup HashMap is a HashMap that can be backed up into remote nodes using fusion.
 * @author Josh
 *
 * @param <K> The type of key, which must be Serializable
 * @param <V> The type of value, which must be NumericalListEncodable
 */
public class FusionBackupHashMap<K extends Serializable, V extends NumericalListEncodable> implements Map<K, V>{
	
	private final ArrayList<Node> aux;
	private final HashMap<K, Node> map;
	private final int id;
	private final CommunicationManager manager;
	private final HashMap<Integer, Integer> id2connection;
	private final HashMap<Integer, Integer> connection2id;
	private final Object mapLock = new Object();
	private final Object updateLock = new Object();
	
	/**
	 * Construct a fusion-backup hashmap with several specified remote backup nodes. If there is available backup, restore
	 * this hashmap from backup; otherwise create a new hashmap.
	 * @param hosts A map which contains id-ip:port pair for backup nodes, for example (2, "192.168.1.1:12345")
	 * @param id the id of this hashmap. Id is the unique identifier to distinguish primaries in backup repository
	 */
	public FusionBackupHashMap(Map<Integer, String> hosts, int id){
		super();
		this.map = new HashMap<K, Node>();
		this.aux = new ArrayList<Node>();
		this.id = id;
		this.manager = new CommunicationManager();
		this.id2connection = new HashMap<Integer, Integer>();
		this.connection2id = new HashMap<Integer, Integer>();
		if(hosts == null)
			return;
		LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
		for (Map.Entry<Integer, String> entry : hosts.entrySet()) {
			final int nodeId = entry.getKey();
			String[] temp = entry.getValue().split(":");
			final String ip = temp[0];
			final int port = Integer.parseInt(temp[1]);
			final Semaphore semaphore = new Semaphore(0);
			semaphores.add(semaphore);
			new Thread() {
				@Override
				public void run() {
					int connection = -1;
					try {
						connection = manager.connect(ip, port);
						manager.sendMessageForResponse(
								connection,
								new Message()
								.put("MessageType", MessageType.CONNECT_REQUEST)
								.put("NodeType", NodeType.CLIENT)
								.put("id",FusionBackupHashMap.this.id),
								new MessageFilter() {
									@Override
									public boolean filter(Message msg) {
										return msg != null && msg.containsKey("MessageType")
												&& msg.get("MessageType") == MessageType.CONNECT_ACCEPTED;
									}
								}, 5000, new OnMessageReceivedListener() {
									@Override
									public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
										synchronized (mapLock) {
											id2connection.put(nodeId, id);
											connection2id.put(id, nodeId);
										}
										manager.setOnMessageReceivedListener(id, new MessageListener());
										System.out.println("Connected to repository node "+ nodeId);
									}

									@Override
									public void OnReceiveError(CommunicationManager manager, int id) {
										manager.closeConnection(id);
									}
								}, true);

					} catch (IOException e) {
						if (connection >= 0)
							manager.closeConnection(connection);
					}
					semaphore.release();
				}
			}.start();
		}
		for (Semaphore s : semaphores)
			try {
				s.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		try {
			recover();
			System.out.println("Recover successful!");
		} catch (RecoverFailureException e) {
			e.printStackTrace();
			System.err.println("Recover fails! Create an empty hash map!");
		}
	}

	/**
	 * Put a key-value pair to the map, and update the backup data at the same time
	 */
	@Override
	public V put(K key, V value) {
		if(containsKey(key) && (value == null && get(key) == null || value != null && value.equals(get(key))))
			return value;
		synchronized(updateLock){
			V prev = putWithoutBackup(key, value);
			ArrayList<Double> prevlist = prev==null? null: prev.encode();
			ArrayList<Double> curlist = value==null? null: value.encode();
			Message msg = new Message().put("MessageType", MessageType.BACKUP_UPDATE).
										put("UpdateType", UpdateType.PUT).
										put("key", key).
										put("prev", prevlist).
										put("cur", curlist);
			Set<Integer> keySet = null;
			synchronized(mapLock){
				keySet = connection2id.keySet();
			}
			for(Integer connection: keySet)
				try {
					manager.sendMessage(connection, msg);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Unable to back up operation!");
				}
			return prev; 
		}
	}
	
	private V putWithoutBackup(K key, V value){
		Node node = null;
		if(map.containsKey(key)){
			node = map.get(key);		
		}else{
			node = new Node(aux.size(), null);
			map.put(key, node);
			aux.add(node);
		}
		V ret = node.val;
		node.val = value;
		return ret;
	}

	/**
	 * Remove a key-value from the map, and update the backup data at the same time
	 */
	@Override
	public V remove(Object key){
		if(!containsKey(key)) return null;
		synchronized(updateLock){
			Node node = map.remove(key);
			if(node == null) return null;
			Node tail = aux.get(aux.size()-1);
			aux.set(node.paux, tail);
			tail.paux = node.paux;
			aux.remove(aux.size()-1);
			
			ArrayList<Double> val = node.val==null? null: node.val.encode();
			ArrayList<Double> end = tail.val==null? null: tail.val.encode();
			Message msg = new Message().put("MessageType", MessageType.BACKUP_UPDATE).
										put("UpdateType", UpdateType.REMOVE).
										put("key", (Serializable)key).
										put("val", val).
										put("end", end);
			Set<Integer> keySet = null;
			synchronized(mapLock){
				keySet = connection2id.keySet();
			}
			for(Integer connection: keySet)
				try {
					manager.sendMessage(connection, msg);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Unable to back up operation!");
				}
			
			return node.val;
		}
	}
	

	/**
	 * Put all key-value pairs from another map into this map, and update the backup data at the same time
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for(Map.Entry<? extends K, ? extends V> entry : m.entrySet())
			put(entry.getKey(), entry.getValue());
	}

	/**
	 * Clear all key-value pairs in this map, and update the backup data at the same time
	 */
	@Override
	public void clear() {
		for(K key: keySet())
			remove(key);
	}
	
	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsValue(Object value) {
		try{
			return map.containsValue(new Node(0, value == null? null: (V)value));
		}catch(ClassCastException e){
			return false;
		}
	}

	@Override
	public V get(Object key) {
		Node node = map.get(key);
		return node==null? null: node.val;
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		LinkedList<V> ret = new LinkedList<V>();
		for(Node node : map.values())
			ret.add(node.val);
		return ret;
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		Set<Map.Entry<K, V>> set = new LinkedHashSet<Map.Entry<K, V>>();
		for(Map.Entry<K, Node> entry : map.entrySet())
			set.add(new MyEntry(entry.getKey(), entry.getValue().val));
		return set;
	}
	
	
	private void recover() throws RecoverFailureException {
		int connection = -1;
		synchronized(mapLock){
			if(connection2id.isEmpty())
				throw new RecoverFailureException("No available backup node!");
			connection = connection2id.keySet().iterator().next();
		}
		final HashMap<String, Boolean> result = new HashMap<String, Boolean>();
		try {
			manager.sendMessageForResponse(connection, 
					new Message().put("MessageType", MessageType.RECOVER_REQUEST), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.RECOVER_RESULT;
						}
					}, 10000,
					new OnMessageReceivedListener(){
						@Override
						public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
							result.put("success", true);
						}
						@Override
						public void OnReceiveError(CommunicationManager manager, int id) {
							result.put("success", false);
						}
					}, true);
		} catch (IOException e) {
			throw new RecoverFailureException("Unable to send RECOVER_REQUEST to repository!");
		}
		if(!(Boolean)result.get("success"))
			throw new RecoverFailureException("Recover fails!");
	}
	
	@Override
	public String toString(){
		return map.toString();
	}
	
	private class Node{
		public int paux;
		public V val;
		
		public Node(int paux, V val){
			this.paux = paux;
			this.val = val;
		}
		
		@Override
		public int hashCode(){
			if(val == null) return 0;
			return val.hashCode();
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object that){
			if(that == null) return false;
			if(!(that instanceof FusionBackupHashMap.Node)) return false;
			Node node = (Node)that;
			if(val == null) return node.val == null;
			return val.equals(node.val);
		}
		
		@Override
		public String toString(){
			return val+"";
		}
	}
	
	private class MyEntry implements Map.Entry<K, V>{
		
		private K key;
		private V val;
		
		public MyEntry(K key, V val){
			this.key = key;
			this.val = val;
		}
		
		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return val;
		}

		@Override
		public V setValue(V value) {
			V old = val;
			val = value;
			return old;
		}

		@Override
		public String toString(){
			return key+"="+val;
		}
		
	}
	
	private class MessageListener implements OnMessageReceivedListener{
	

		@Override
		public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("MessageListener: Error occurs when receiving message!");
			manager.closeConnection(id);
			synchronized(mapLock){
				int nodeId = connection2id.get(id);
				connection2id.remove(id);
				id2connection.remove(nodeId);
			}
		}
		
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------
	//Following code is for testing!
	
	private static class EncodableInteger implements NumericalListEncodable{
		
		public final int val;
		
		public EncodableInteger(int val){
			this.val = val;
		}

		@Override
		public ArrayList<Double> encode() {
			ArrayList<Double> ret = new ArrayList<Double>();
			ret.add((double)(val==0? Integer.MIN_VALUE: val));
			return ret;
		}

		
		@SuppressWarnings("unused")
		public static EncodableInteger decode(ArrayList<Double> numericalList) {
			if(numericalList.size() == 0)
				throw new DecodeException("Unable to decode from empty list!");
			int result = numericalList.get(0).intValue();
			result = result==Integer.MIN_VALUE? 0: result;
			return new EncodableInteger(result);
		}
		
		@Override
		public int hashCode(){
			return val;
		}
		
		@Override
		public boolean equals(Object obj){
			if(obj == null) return false;
			if(!(obj instanceof EncodableInteger)) return false;
			return ((EncodableInteger)obj).val == val;
		}
		
		@Override
		public String toString(){
			return val+"";
		}
	}

	private static <K, V> void methodTest(Map<K, V> hm, Map<K, V> fm){
		assert(hm.size() == fm.size());
		assert(fm.isEmpty() == hm.isEmpty());
		Iterator<Map.Entry<K, V>> hi = hm.entrySet().iterator();
		Iterator<Map.Entry<K, V>> fi = fm.entrySet().iterator();
		while(hi.hasNext()){
			Map.Entry<K, V> e1 = hi.next();
			Map.Entry<K, V> e2 = fi.next();
			assert(e1.getKey() == e2.getKey());
			assert(e1.getValue() == e2.getValue());
		}
		Iterator<K> hki = hm.keySet().iterator();
		Iterator<K> fki = hm.keySet().iterator();
		while(hki.hasNext()){
			K k1 = hki.next();
			K k2 = fki.next();
			assert(k1 == k2);
			assert(hm.get(k1) == fm.get(k2));
			assert(hm.containsKey(k1) == fm.containsKey(k2));
		}
		Iterator<V> hvi = hm.values().iterator();
		Iterator<V> fvi = fm.values().iterator();
		while(hvi.hasNext()){
			V v1 = hvi.next();
			V v2 = fvi.next();
			assert(v1 == v2);
			assert(hm.containsValue(v1) == fm.containsValue(v2));
		}
		assert(hm.toString().equals(fm.toString()));
	}
	
	private static void testPut(HashMap<String, EncodableInteger> hm, FusionBackupHashMap<String, EncodableInteger> fm, int testCase){
		System.out.println("Testing put ...");
		List<FusionBackupHashMap<String, EncodableInteger>.Node> aux = fm.aux;
		for(int i=0; i<testCase; i++){
			String k = "key"+(int)(Math.random()*testCase/2);
			EncodableInteger v = new EncodableInteger((int)(Math.random()*testCase/2));
			hm.put(k, v);
			fm.put(k, v);
		}
		EncodableInteger _4 = new EncodableInteger(4);
		hm.put(null, _4);
		fm.put(null, _4);
		hm.put("28", null);
		fm.put("28", null);
		methodTest(hm, fm);	
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testRemove(HashMap<String, EncodableInteger> hm, FusionBackupHashMap<String, EncodableInteger> fm, int testCase){
		System.out.println("Testing remove ...");
		List<FusionBackupHashMap<String, EncodableInteger>.Node> aux = fm.aux;
		for(int i=0; i<testCase; i++){
			String k = "str"+(int)(Math.random()*testCase/2);
			FusionBackupHashMap<String, EncodableInteger>.Node tail = null;
			if(!aux.isEmpty())
				tail = aux.get(aux.size()-1);
			int kIndex = 0;
			boolean pre = fm.containsKey(k);
			if(pre)
				kIndex = fm.map.get(k).paux;
			assert(hm.remove(k) == fm.remove(k));
			if(pre != fm.containsKey(k)){
				assert(tail.paux == kIndex);
				assert(aux.get(kIndex) == tail);
			}
		}
		methodTest(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testPutAll(HashMap<String, EncodableInteger> hm, FusionBackupHashMap<String, EncodableInteger> fm, int testCase){
		System.out.println("Testing put all ...");
		List<FusionBackupHashMap<String, EncodableInteger>.Node> aux = fm.aux;
		Map<String, EncodableInteger> add = new HashMap<String, EncodableInteger>();
		testCase = 10000;
		for(int i=0; i<testCase; i++){
			String k = "key"+(int)(Math.random()*testCase/2);
			EncodableInteger v = new EncodableInteger((int)(Math.random()*testCase/2));
			add.put(k, v);
		}
		hm.putAll(add);
		fm.putAll(add);
		methodTest(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testClear(HashMap<String, EncodableInteger> hm, FusionBackupHashMap<String, EncodableInteger> fm){
		System.out.println("Testing clear ...");
		List<FusionBackupHashMap<String, EncodableInteger>.Node> aux = fm.aux;
		hm.clear();
		fm.clear();
		methodTest(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
//		HashMap<String, EncodableInteger> hm = new HashMap<String, EncodableInteger>();
//		FusionBackupHashMap<String, EncodableInteger> fm = new FusionBackupHashMap<String, EncodableInteger>(null, 0);
//		int testRound = 100;
//		for(int i=0; i<testRound; i++){
//			int testType = (int)(Math.random()*4);
//			switch(testType){
//				case 0:
//					testPut(hm ,fm, 5000+(int)(Math.random()*10000));
//					break;
//				case 1:
//					testRemove(hm ,fm, 2000+(int)(Math.random()*5000));
//					break;
//				case 2:
//					testPutAll(hm ,fm, 5000+(int)(Math.random()*10000));
//					break;
//				case 3:
//					testClear(hm, fm);
//					break;
//			}
//		}			
//		
//		System.out.println("Pass all tests!");
		
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		final FusionBackupHashMap<String, EncodableInteger> m0 = new FusionBackupHashMap<String, EncodableInteger>(cluster, 0);
		final FusionBackupHashMap<String, EncodableInteger> m1 = new FusionBackupHashMap<String, EncodableInteger>(cluster, 1);
		new Thread(){
			@Override
			public void run(){
				for(int i=0; i<1000; i++){
					String k = "str"+(int)(Math.random()*500);
					EncodableInteger v = new EncodableInteger((int)(Math.random()*1000));
					m0.put(k, v);
				}
				for(int i=0; i<1000; i++){
					String k = "str"+(int)(Math.random()*500);
					m0.remove(k);
				}
			}
		}.start();
		new Thread(){
			@Override
			public void run(){
				for(int i=0; i<1000; i++){
					String k = "str"+(int)(Math.random()*500);
					EncodableInteger v = new EncodableInteger((int)(Math.random()*1000));
					m1.put(k, v);
				}
				for(int i=0; i<1000; i++){
					String k = "str"+(int)(Math.random()*500);
					m1.remove(k);
				}
			}
		}.start();
	}

}
