package dataStructures;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.*;
import java.util.concurrent.Semaphore;

import backups.DataEntry;
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
public class FusionBackupHashMap<K extends Serializable, V> implements Map<K, V>{
	
	private final ArrayList<Node> aux;
	private final HashMap<K, Node> map;
	private final int id;
	private final CommunicationManager manager;
	private final HashMap<Integer, Integer> id2connection;
	private final HashMap<Integer, Integer> connection2id;
	private final Object mapLock = new Object();
	private final Object updateLock = new Object();
	private final Coder<V> coder;
	private final PauseThread pauseThread = new PauseThread();
	
	/**
	 * Construct a fusion-backup hashmap with several specified remote backup nodes. If there is available backup, restore
	 * this hashmap from backup; otherwise create a new hashmap.
	 * @param hosts A map which contains id-ip:port pair for backup nodes, for example (2, "192.168.1.1:12345")
	 * @param id the id of this hashmap. Id is the unique identifier to distinguish primaries in backup repository
	 */
	public FusionBackupHashMap(Map<Integer, String> hosts, int id, Coder<V> coder){
		super();
		if(coder == null)
			throw new IllegalArgumentException("Coder must be specified!");
		if(id < 0)
			throw new IllegalArgumentException("Illegal id "+id);
		this.map = new HashMap<K, Node>();
		this.aux = new ArrayList<Node>();
		this.id = id;
		this.coder = coder;
		this.manager = new CommunicationManager();
		this.id2connection = new HashMap<Integer, Integer>();
		this.connection2id = new HashMap<Integer, Integer>();
		this.pauseThread.start();
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
								}, 10000, new OnMessageReceivedListener() {
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
		if(containsKey(key) && equals(value, get(key)))
			return value;
		synchronized(updateLock){
			Node node = null;
			if(map.containsKey(key)){
				node = map.get(key);		
			}else{
				node = new Node(aux.size(), null);
				map.put(key, node);
				aux.add(node);
			}
			V prev = node.val;
			node.val = value;
			ArrayList<Double> prevlist = coder.encode(prev);
			ArrayList<Double> curlist = coder.encode(value);
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
	

	/**
	 * Remove a key-value from the map, and update the backup data at the same time
	 */
	@Override
	public V remove(Object key){
		if(!containsKey(key)) return null;
		synchronized(updateLock){
			Node node = map.remove(key);
			assert(node != null);
			Node tail = aux.get(aux.size()-1);
			aux.set(node.paux, tail);
			tail.paux = node.paux;
			aux.remove(aux.size()-1);
			
			ArrayList<Double> val = coder.encode(node.val);
			ArrayList<Double> end = coder.encode(tail.val);
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
		HashSet<K> keys = new HashSet<K>();
		keys.addAll(keySet());
		for(K key: keys)
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
		HashSet<Integer> connections = new HashSet<Integer>();
		synchronized(mapLock){
			if(connection2id.isEmpty())
				throw new RecoverFailureException("No available backup node!");
			connections.addAll(connection2id.keySet());
		}
		for(final int connection: connections){
			final HashMap<String, Object> result = new HashMap<String, Object>();
			try {
				manager.sendMessageForResponse(connection, 
						new Message().put("MessageType", MessageType.RECOVER_REQUEST), 
						new MessageFilter(){
							@Override
							public boolean filter(Message msg) {
								return msg != null && msg.containsKey("MessageType")
										&& msg.get("MessageType") == MessageType.RECOVER_RESULT;
							}
						}, 20000,
						new OnMessageReceivedListener(){		
							@SuppressWarnings("unchecked")
							@Override
							public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
								if(!(Boolean)msg.get("success")){
									result.put("success", false);
									result.put("reason", msg.get("reason"));
									return;
								}
								ArrayList<DataEntry<Serializable, ArrayList<Double>>> recover = 
										(ArrayList<DataEntry<Serializable, ArrayList<Double>>>) msg.get("result");
								assert(recover != null);
								for(int i=0; i<recover.size(); i++){
									DataEntry<Serializable, ArrayList<Double>> entry = recover.get(i);
									Node n = new Node(i, coder.decode(entry.value));
									map.put((K)entry.key, n);
									aux.add(n);
								}
								result.put("success", true);
							}
							@Override
							public void OnReceiveError(CommunicationManager manager, int id) {
								result.put("success", false);
								result.put("reason", "OnReceiveError");
							}
						}, true);
			} catch (IOException e) {
				throw new RecoverFailureException("Unable to send RECOVER_REQUEST to repository!");
			}
			if((Boolean)result.get("success"))
				return;
		}
		throw new RecoverFailureException("Recover fails !");
	}
	
	@Override
	public String toString(){
		return map.toString();
	}
	
	private static boolean equals(Object a, Object b){
		return (a==null && b==null) || (a!=null && a.equals(b));
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
		public void OnMessageReceived(final CommunicationManager manager, final int id, Message msg) {
			if(!msg.containsKey("MessageType")) return;
			int nodeId = -1;
			synchronized(mapLock){
				nodeId = connection2id.get(id);
			}
			try {
				switch((MessageType)msg.get("MessageType")){
					case PAUSE_UPDATE:
						pauseThread.pauseUpdate();
						new Thread(){
							@Override
							public void run(){
								HashSet<Integer> connections = new HashSet<Integer>();
								synchronized(connection2id){
									connections.addAll(connection2id.keySet());
								}
								LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
								for(int connection: connections){
									final Semaphore s = new Semaphore(0);
									semaphores.add(s);
									try {
										manager.sendMessageForResponse(connection, new Message().put("MessageType", MessageType.SIGNAL),
																	   new MessageFilter(){
																		@Override
																		public boolean filter(Message msg) {
																			return msg!=null && msg.containsKey("MessageType")
																					&& msg.get("MessageType") == MessageType.SIGNAL_ACK;
																			}
																		}, 5000, 
																		new OnMessageReceivedListener(){
																			@Override
																			public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
																				s.release();
																			}
																			@Override
																			public void OnReceiveError(CommunicationManager manager, int id) {
																				s.release();
																			}
																		}, false);
									} catch (IOException e) {
										s.release();
									}
								}
								for(Semaphore semaphore: semaphores)
									try {
										semaphore.acquire();
									} catch (InterruptedException e) {
										e.printStackTrace();
										System.err.println("Caught unexpected interruption!");
									}
								try {
									manager.sendMessageForResponse(id, new Message().put("MessageType", MessageType.UPDATE_PAUSED), 
											   new MessageFilter(){
												@Override
												public boolean filter(Message msg) {
													return msg != null && msg.containsKey("MessageType")
															&& msg.get("MessageType") == MessageType.RESUME_UPDATE;
												}
												}, 20000, 
												new OnMessageReceivedListener(){
													@Override
													public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
														pauseThread.resumeUpdate();		
													}
													@Override
													public void OnReceiveError(CommunicationManager manager, int id) {
														System.err.println("Unable to receive RESUME_UPDATE!");
														pauseThread.resumeUpdate();
													}
												}, false);
								} catch (IOException e) {
									System.err.println("Unable to send UPDATE_PAUSED!");
									pauseThread.resumeUpdate();
								}
							}
						}.start();	
						break;
					case DATA_REQUEST:
						ArrayList<ArrayList<Double>> data = new ArrayList<ArrayList<Double>>();
						for(Node node: aux)
							data.add(coder.encode(node.val));
						manager.sendMessage(id, new Message().put("MessageType", MessageType.DATA_RESPONSE).
															  put("data", data));
						break;
					case EXCEPTION:
						System.err.println("ClusterMessageListener: Repository "+nodeId+" has internal error: "+msg.get("Exception"));
					default:
						break;
				}
			}catch(Exception e){
				e.printStackTrace();
				try {
					manager.sendMessage(id, new Message().put("MessageType", MessageType.EXCEPTION).put("Exception", e));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
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
	
	private class PauseThread extends Thread{
		
		private int pauseNum = 0;
		private Object pauseNumLock = new Object();
		private Object isPausedLock = new Object();
		private boolean isPaused = false;
		
		public void pauseUpdate(){
			synchronized(pauseNumLock){
				pauseNum ++;
				pauseNumLock.notifyAll();
			}
			synchronized(isPausedLock){
				while(!isPaused)
					try {
						isPausedLock.wait();
					} catch (InterruptedException e) {}
			}
		}
		
		public void resumeUpdate(){
			synchronized(pauseNumLock){
				pauseNum --;
				pauseNumLock.notifyAll();
			}
		}
		
		@Override
		public void run(){
			while(true){
				synchronized(pauseNumLock){
					while(pauseNum <= 0)
						try {
							pauseNumLock.wait();
						} catch (InterruptedException e) {}
				}
				synchronized(updateLock){
					synchronized(isPausedLock){
						isPaused = true;
						isPausedLock.notifyAll();
					}
					synchronized(pauseNumLock){
						while(pauseNum > 0)
							try {
								pauseNumLock.wait();
							} catch (InterruptedException e) {}
					}
				}
				synchronized(isPausedLock){
					isPaused = false;
					isPausedLock.notifyAll();
				}
			}
		}
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------
	//Following code is for testing!
	
	private static class IntegerCoder implements Coder<Integer>{
		
		private int toInt(double d){
			return (int)Math.round(d);
		}

		@Override
		public ArrayList<Double> encode(Integer target) {
			ArrayList<Double> ret = new ArrayList<Double>();
			if(target == null){
				ret.add(1.0);
				ret.add(1.0);
			}else
				ret.add((double)target);
			return ret;
		}


		@Override
		public Integer decode(ArrayList<Double> source) {
			if(source == null || source.size() == 0)
				throw new DecodeException("Unable to decode from empty list!");
			if(source.size()>1 && toInt(source.get(0)) == 1 && toInt(source.get(1)) == 1)
				return null;
			return toInt(source.get(0));
		}
	}
	

	private static <K, V> void testEquality(Map<K, V> hm, Map<K, V> fm){
		assert(hm.size() == fm.size());
		assert(fm.isEmpty() == hm.isEmpty());
		Set<Map.Entry<K, V>> hes = hm.entrySet();
		Set<Map.Entry<K, V>> fes = fm.entrySet();
		assert(hes.size() == fes.size());
		for(Map.Entry<K, V> entry: hes){
			assert(fm.containsKey(entry.getKey()));
			assert(hm.containsKey(entry.getKey()));
			assert(equals(hm.get(entry.getKey()), fm.get(entry.getKey()))): hm.get(entry.getKey())+" vs "+fm.get(entry.getKey());
			assert(fm.containsValue(entry.getValue()));
			assert(hm.containsValue(entry.getValue()));
		}
		for(Map.Entry<K, V> entry: fes){
			assert(fm.containsKey(entry.getKey()));
			assert(hm.containsKey(entry.getKey()));
			assert(equals(hm.get(entry.getKey()), fm.get(entry.getKey()))): hm.get(entry.getKey())+" vs "+fm.get(entry.getKey());
			assert(fm.containsValue(entry.getValue()));
			assert(hm.containsValue(entry.getValue()));
		}
		Set<K> hks = hm.keySet();
		Set<K> fks = hm.keySet();
		assert(hks.size() == fks.size());
		for(K k : hks){
			assert(fks.contains(k));
			assert(equals(hm.get(k), fm.get(k))): hm.get(k)+" vs "+fm.get(k);
			assert(hks.contains(k));
		}
		for(K k : fks){
			assert(fks.contains(k));
			assert(equals(hm.get(k), fm.get(k))): hm.get(k)+" vs "+fm.get(k);
			assert(hks.contains(k));
		}
		Collection<V> hvc = hm.values();
		Collection<V> fvc = fm.values();
		assert(hvc.size() == fvc.size());
		for(V v: hvc){
			assert(hm.containsValue(v));
			assert(fm.containsValue(v));
		}
		for(V v: fvc){
			assert(hm.containsValue(v));
			assert(fm.containsValue(v));
		}
	}
	
	private static void testPut(HashMap<String, Integer> hm, FusionBackupHashMap<String, Integer> fm, int testCase){
		System.out.println("Testing put ...");
		List<FusionBackupHashMap<String, Integer>.Node> aux = fm.aux;
		for(int i=0; i<testCase; i++){
			String k = "key"+(int)(Math.random()*testCase/2);
			int v = (int)(Math.random()*testCase/2);
			hm.put(k, v);
			fm.put(k, v);
		}
		hm.put(null, 4);
		fm.put(null, 4);
		hm.put("28", null);
		fm.put("28", null);
		testEquality(hm, fm);	
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testRemove(HashMap<String, Integer> hm, FusionBackupHashMap<String, Integer> fm, int testCase){
		System.out.println("Testing remove ...");
		List<FusionBackupHashMap<String, Integer>.Node> aux = fm.aux;
		for(int i=0; i<testCase; i++){
			String k = "str"+(int)(Math.random()*testCase/2);
			FusionBackupHashMap<String, Integer>.Node tail = null;
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
		testEquality(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testPutAll(HashMap<String, Integer> hm, FusionBackupHashMap<String, Integer> fm, int testCase){
		System.out.println("Testing put all ...");
		List<FusionBackupHashMap<String, Integer>.Node> aux = fm.aux;
		Map<String, Integer> add = new HashMap<String, Integer>();
		testCase = 10000;
		for(int i=0; i<testCase; i++){
			String k = "key"+(int)(Math.random()*testCase/2);
			int v = (int)(Math.random()*testCase/2);
			add.put(k, v);
		}
		hm.putAll(add);
		fm.putAll(add);
		testEquality(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static void testClear(HashMap<String, Integer> hm, FusionBackupHashMap<String, Integer> fm){
		System.out.println("Testing clear ...");
		List<FusionBackupHashMap<String, Integer>.Node> aux = fm.aux;
		hm.clear();
		fm.clear();
		testEquality(hm, fm);
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
	}
	
	private static FusionBackupHashMap<String, Integer> testRecover(HashMap<String, Integer> hm, FusionBackupHashMap<String, Integer> fm, HashMap<Integer, String> cluster, int id){
		System.out.println("Testing recovery ...");
		HashSet<Integer> connections = new HashSet<Integer>();
		connections.addAll(fm.connection2id.keySet());
		for(int connection: connections)
			fm.manager.closeConnection(connection);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		fm = new FusionBackupHashMap<String, Integer>(cluster, id, new IntegerCoder());
		testEquality(hm, fm);
		List<FusionBackupHashMap<String, Integer>.Node> aux = fm.aux;
		for(int i=0; i<aux.size(); i++)
			assert(aux.get(i).paux == i);
		assert(aux.size() == fm.size());
		System.out.println("pass!");
		return fm;
	}
	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		final HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		final int maxFault = 4;
		final int total = 4;
		final int testRound = 20;
		final Semaphore falutSemaphore = new Semaphore(maxFault);
		final Semaphore terminateSemaphore = new Semaphore(1-total);
		for(int n=0; n<total; n++){
			final int No = n;
			new Thread(){
				@Override
				public void run(){
					try{
					HashMap<String, Integer> hm = new HashMap<String, Integer>();
					FusionBackupHashMap<String, Integer> fm = new FusionBackupHashMap<String, Integer>(cluster, No, new IntegerCoder());
					for(int i=0; i<testRound; i++){
						int testType = (int)(Math.random()*4);
						switch(testType){
							case 0:
								testPut(hm ,fm, 500+(int)(Math.random()*1000));
								falutSemaphore.acquire();
								fm = testRecover(hm, fm, cluster, No);
								falutSemaphore.release();
								break;
							case 1:
								testRemove(hm ,fm, 200+(int)(Math.random()*500));
								falutSemaphore.acquire();
								fm = testRecover(hm, fm, cluster, No);
								falutSemaphore.release();
								break;
							case 2:
								testPutAll(hm ,fm, 500+(int)(Math.random()*1000));
								falutSemaphore.acquire();
								fm = testRecover(hm, fm, cluster, No);
								falutSemaphore.release();
								break;
							case 3:
								testClear(hm, fm);
								falutSemaphore.acquire();
								fm = testRecover(hm, fm, cluster, No);
								falutSemaphore.release();
								break;
						}
					}
					System.out.println("Primary "+No+" pass all tests!");
					terminateSemaphore.release();
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}.start();
		}
		terminateSemaphore.acquire();
		System.out.println("Congratulations! All test cases has been passed!");
	}

}
