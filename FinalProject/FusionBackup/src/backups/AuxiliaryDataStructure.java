package backups;

import java.io.Serializable;

/**
 * AuxiliaryDataStructure is the data structure that is used to represent the data structuer of a primary.
 *
 * @author Josh
 *
 */
public interface AuxiliaryDataStructure{

	/**
	 * Add a new auxiliary node and its key into this data structure
	 * @param key the key
	 * @param node the added auxiliary node
	 */
	public void put(Serializable key, AuxiliaryNode node);
	
	/**
	 * Check if a key is already in this data structure
	 * @param key the key
	 * @return if it is in this data structure
	 */
	public boolean containsKey(Serializable key);

	/**
	 * Remove an auxiliary node from this auxiliary data structure
	 * @param key the key to remove
	 * @return the removed node
	 */
	public AuxiliaryNode remove(Serializable key);
	
	/**
	 * Get an auxiliary node
	 * @param key the key
	 * @return the auxiliary node
	 */
	public AuxiliaryNode get(Serializable key);
	

	/**
	 * Get the size of this data structure
	 * @return the size
	 */
	public int size();
}
