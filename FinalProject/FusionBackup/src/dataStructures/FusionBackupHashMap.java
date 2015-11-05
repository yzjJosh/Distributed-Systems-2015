package dataStructures;

import interfaces.Backupable;

import java.io.Serializable;
import java.util.*;

import org.jblas.DoubleMatrix;

import exceptions.RecoverFailureException;

/**
 * Fusion-backup HashMap is a HashMap that can be backed up into remote nodes using fusion.
 * @author Josh
 *
 * @param <K> The type of key, which must be serializable
 * @param <V> The type of value, which must be serializable
 */
public class FusionBackupHashMap<K extends Serializable, V extends Serializable> implements Map<K, V>, Backupable{
	
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
		for(Map.Entry<? extends K, ? extends V> entry : m.entrySet()){
			Node node = null;
			if(map.containsKey(entry.getKey())){
				node = map.get(entry.getKey());		
			}else{
				node = new Node(aux.size(), null);
				map.put(entry.getKey(), node);
				aux.add(node);
			}
			node.val = entry.getValue();
		}
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
		Set<Map.Entry<K, V>> set = new HashSet<Map.Entry<K, V>>();
		for(Map.Entry<K, Node> entry : map.entrySet())
			set.add(new MyEntry(entry.getKey(), entry.getValue().val));
		return set;
	}
	
	

	@Override
	public int activeBackupNodes() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void recover() throws RecoverFailureException {
		// TODO Auto-generated method stub
		
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

		
	}
	

	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args){
		DoubleMatrix a = new DoubleMatrix(new double[][]{
				{1,2,3},
				{4,5,6}
		});
		DoubleMatrix b = new DoubleMatrix(new double[][]{
				{1,2},
				{4,5},
				{6,7}
		});
		System.out.println(a.transpose());
	}

}
