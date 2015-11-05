package dataStructures;

import interfaces.NumericalListEncodable;
import interfaces.Recoverable;

import java.io.Serializable;
import java.util.*;

import exceptions.DecodeException;
import exceptions.RecoverFailureException;

/**
 * Fusion-backup HashMap is a HashMap that can be backed up into remote nodes using fusion.
 * @author Josh
 *
 * @param <K> The type of key, which must be Serializable
 * @param <V> The type of value, which must be NumericalListEncodable
 */
public class FusionBackupHashMap<K extends Serializable, V extends NumericalListEncodable> implements Map<K, V>, Recoverable{
	
	private final ArrayList<Node> aux;
	private final Map<String, Integer> hosts;
	private final HashMap<K, Node> map;
	
	/**
	 * Construct a fusion-backup hashmap with several specified remote backup nodes.
	 * @param hosts A map which contains ipaddr-port# pairs of backup nodes.
	 */
	public FusionBackupHashMap(Map<String, Integer> hosts){
		super();
		this.hosts = hosts;
		this.map = new HashMap<K, Node>();
		this.aux = new ArrayList<Node>();
	}

	/**
	 * Put a key-value pair to the map, and update the backup data at the same time
	 */
	@Override
	public V put(K key, V value) {
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
	public V remove(Object key) {
		Node node = map.remove(key);
		if(node == null) return null;
		Node tail = aux.get(aux.size()-1);
		aux.set(node.paux, tail);
		tail.paux = node.paux;
		aux.remove(aux.size()-1);
		return node.val;
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
		map.clear();
		aux.clear();
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
	
	
	@Override
	public void recover() throws RecoverFailureException {
		// TODO Auto-generated method stub
		
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
			ret.add((double)val);
			return ret;
		}

		@Override
		public NumericalListEncodable decode(ArrayList<Double> numericalList) {
			if(numericalList.size() == 0)
				throw new DecodeException("Unable to decode from empty list!");
			return new EncodableInteger(numericalList.get(0).intValue());
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
	public static void main(String[] args){
		HashMap<String, EncodableInteger> hm = new HashMap<String, EncodableInteger>();
		FusionBackupHashMap<String, EncodableInteger> fm = new FusionBackupHashMap<String, EncodableInteger>(null);
		int testRound = 100;
		for(int i=0; i<testRound; i++){
			int testType = (int)(Math.random()*4);
			switch(testType){
				case 0:
					testPut(hm ,fm, 5000+(int)(Math.random()*10000));
					break;
				case 1:
					testRemove(hm ,fm, 2000+(int)(Math.random()*5000));
					break;
				case 2:
					testPutAll(hm ,fm, 5000+(int)(Math.random()*10000));
					break;
				case 3:
					testClear(hm, fm);
					break;
			}
		}			
		
		System.out.println("Pass all tests!");
	}

}
