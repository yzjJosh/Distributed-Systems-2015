package communication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Message is the communication unit transfered between nodes
 * @author Josh
 *
 */
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final HashMap<String, Serializable> data = new HashMap<String, Serializable>();
	
	private long crc = 0; //The crc which is used to verify this message.
	
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
	
	private long calculateCRC(){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		byte[] bytes = null;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(data);
			bytes = bos.toByteArray();
			out.close();bos.close();
		} catch(IOException e){
			e.printStackTrace();
		}
		if(bytes == null) return 0;
		CRC32 CRC = new CRC32();
		CRC.update(bytes);
		return CRC.getValue();
	}
	
	/**
	 * Add CRC32 code into this message, which can be verified later
	 */
	public void addCRC(){
		crc = calculateCRC();
	}
	
	/**
	 * Verify the CRC32 code of this message
	 * @return if this message is right or not
	 */
	public boolean verifyCRC(){
		return crc == calculateCRC();
	}
	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args){
		Message msg = new Message();
		for(int i=0; i<10000; i++){
			int rand = (int)Math.random()*5000;
			msg.put(""+rand, rand);
		}
		msg.addCRC();
		assert(msg.verifyCRC());
		msg.put("3", 4);
		assert(!msg.verifyCRC());
		System.out.println("Pass!");
	}

}
