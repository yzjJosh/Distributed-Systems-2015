package communication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
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
	private int checkSum = 0; // The checksum is used to verify this message.
	
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
	
	/**
	 * Add CRC32 code into this message, which can be verified later
	 */
	public void addVerification(){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = null;
		byte[] bytes = null;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(data);
			bytes = bos.toByteArray();
			out.close();
			bos.close();
		} catch(IOException e){
			e.printStackTrace();
		}
		CRC32 CRC = new CRC32();
		CRC.update(bytes);
		crc = CRC.getValue();
		checkSum = 0;
		for(byte b: bytes)
			checkSum += b;
	}
	
	/**
	 * Verify the correctness of this message
	 * @return if this message is right or not
	 */
	public boolean verify(){
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
		CRC32 CRC = new CRC32();
		CRC.update(bytes);
		if(crc != CRC.getValue())
			return false;
		int checksum = 0;
		for(byte b: bytes)
			checksum += b;
		return checksum == checkSum;
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
		msg.addVerification();
		assert(msg.verify());
		msg.put(null, null);
		assert(!msg.verify());
		msg.addVerification();
		assert(msg.verify());
		HashSet<Long> set = new HashSet<Long>();
		msg.put("set", set);
		set.add(44432432L);
		assert(!msg.verify());
		msg.addVerification();
		assert(msg.verify());
		System.out.println("Pass!");
	}

}
