package backups;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class AuxiliaryHashMap<K extends Serializable> implements AuxiliaryDataStructure<K> {
	
	private HashMap<K, AuxiliaryNode> map;
	
	public AuxiliaryHashMap(){
		this.map = new HashMap<K, AuxiliaryNode>();
	}

	@Override
	public void put(K key, AuxiliaryNode node) {
		assert(!map.containsKey(key));
		assert(node != null);
		map.put(key, node);
	}

	@Override
	public AuxiliaryNode remove(K key) {
		assert(map.containsKey(key));
		return map.remove(key);
	}

	@Override
	public AuxiliaryNode get(K key) {
		assert(map.containsKey(key));
		return map.get(key);
	}

	@Override
	public Iterable<DataEntry<K, AuxiliaryNode>> entries() {
		LinkedList<DataEntry<K, AuxiliaryNode>> ret = new LinkedList<DataEntry<K, AuxiliaryNode>>();
		for(Map.Entry<K, AuxiliaryNode> entry : map.entrySet())
			ret.add(new DataEntry<K, AuxiliaryNode>(entry.getKey(), entry.getValue()));
		return ret;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean containsKey(K key) {
		return map.containsKey(key);
	}
	
	@Override
	public String toString(){
		return map.toString();
	}

}
