package backups;

import java.io.Serializable;

/**
 * Data entry is the key-value pairs that are used to back up data.
 * @author Josh
 *
 * @param <K> the key
 * @param <V> the value
 */
public class DataEntry<K extends Serializable, V extends Serializable> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public final K key;
	public final V value;
	
	public DataEntry(K key, V value){
		this.key = key;
		this.value = value;
	}

}
