package backups;

import java.io.Serializable;

/**
 * AuxiliaryDataStructure is the data structure that is used to represent the data structuer of a primary.
 *
 * @author Josh
 *
 * @param <K> The type of key
 */
public interface AuxiliaryDataStructure<K extends Serializable> {

	/**
	 * Add a new auxiliary node and its key into this data structure
	 * @param key the key
	 * @param node the added auxiliary node
	 */
	public void put(K key, AuxiliaryNode node);

	/**
	 * Remove an auxiliary node from this auxiliary data structure
	 * @param key the key to remove
	 * @return the removed node
	 */
	public AuxiliaryNode remove(K key);
	
	/**
	 * Get an auxiliary node
	 * @param key the key
	 * @return the auxiliary node
	 */
	public AuxiliaryNode get(K key);
	
	public Iterable<DataEntry<K, AuxiliaryNode>> entries();
}
