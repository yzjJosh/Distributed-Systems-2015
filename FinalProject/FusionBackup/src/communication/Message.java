package communication;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Message is the communication unit transfered between nodes
 * @author Josh
 *
 */
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final HashMap<String, Serializable> data = new HashMap<String, Serializable>();
	
	/**
	 * Put an entry into this message
	 * @param key the key
	 * @param value the value
	 * @return this message
	 */
	public Message put(String key, Serializable value){
		data.put(key, value);
		return this;
	}
	
	/**
	 * Get a value from key
	 * @param key the key
	 * @return the value
	 */
	public Serializable get(String key){
		return data.get(key);
	}
	
	/**
	 * Check if a key is in this message
	 * @param key the key
	 * @return if the key exists or not
	 */
	public boolean containsKey(String key){
		return data.containsKey(key);
	}
	
	/**
	 * Get the entry set of this message
	 * @return the entry set
	 */
	public Set<Map.Entry<String, Serializable>> entrySet(){
		return data.entrySet();
	}
	
	@Override
	public String toString(){
		return data.toString();
	}

}
