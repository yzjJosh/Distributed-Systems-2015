package message;

import java.io.Serializable;

import server.Clock;



/**
 * Message is the object transfered between processes.
 *
 */

public class Message implements Serializable, Comparable<Message> {
	
	private static final long serialVersionUID = 1L;
	public final MessageType type; 			//The type of this message. Different types are defined above.
	public final Clock clk;					//The timestamp of the sent process. If message is sent from client, this field should be null.
	public final Serializable content;		//The content of this message.
	
	/**
	 * Create a new message.
	 * @param type The type of message.
	 * @param content The content.
	 * @param pid	The sender's pid.
	 */
	public Message(MessageType type, Serializable content, Clock clk){
		this.type = type;
		this.content = content;
		this.clk = clk;
	}

	@Override
	public int compareTo(Message o) {
		if(clk != null && o != null)
			return clk.compareTo(o.clk);
		else
			return 0;
	}
}
