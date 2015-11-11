package backups;

import java.io.Serializable;
import java.util.HashMap;

public class AuxiliaryHashMap implements AuxiliaryDataStructure{
	
	private HashMap<Serializable, AuxiliaryNode> map;
	
	public AuxiliaryHashMap(){
		this.map = new HashMap<Serializable, AuxiliaryNode>();
	}

	@Override
	public void put(Serializable key, AuxiliaryNode node) {
		assert(!map.containsKey(key));
		assert(node != null);
		map.put(key, node);
	}

	@Override
	public AuxiliaryNode remove(Serializable key) {
		assert(map.containsKey(key));
		return map.remove(key);
	}

	@Override
	public AuxiliaryNode get(Serializable key) {
		assert(map.containsKey(key));
		return map.get(key);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean containsKey(Serializable key) {
		return map.containsKey(key);
	}
	
	@Override
	public String toString(){
		return map.toString();
	}

}
