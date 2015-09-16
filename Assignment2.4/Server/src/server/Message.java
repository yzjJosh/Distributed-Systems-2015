package server;

import java.io.Serializable;

/**
 * Type of a message
 *
 */
enum MessageType {
	CLOCK_MESSAGE,			//The message used to update clock between processes.
	TCS_REQUEST_READ,		//The message used to request a critial section for reading.
	CS_REQUEST_WRITE ,		//The message used to request a critial section for writing.
	ACKNOWLEDGE ,			//The message used to respond to a cs request.
	CS_RELEASE,				//The message used to release a critical section.
	RESERVE_SEATE,			//The message used for a client to request the server to reserve seates.
	SEARCH_SEATE,			//The message used for a client to request the server to search seates reserved by a name.
	DELETE_SEATE ,			//The message used for a client to request the server to release seates reserved by a name.
	RESPOND_TO_CLIENT,		//The message used for server to respond to a client.
	SERVER_SYNC				//The message used for server to sychronize seate information with other servers.			
}

/**
 * Message is the object transfered between processes.
 *
 */

public class Message implements Serializable, Comparable<Message> {
	
	private static final long serialVersionUID = 1L;
	public final MessageType type; 		//The type of this message. Different types are defined above.
	public final int pid;					//The pid of sender.
	public final Serializable content;		//The content of this message.
	
	/**
	 * Create a new message.
	 * @param type The type of message.
	 * @param content The content.
	 * @param pid	The sender's pid.
	 */
	public Message(MessageType type, Serializable content, int pid){
		this.type = type;
		this.content = content;
		this.pid = pid;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int compareTo(Message o) {
		if(content instanceof Comparable && o.content instanceof Comparable)
			return ((Comparable)content).compareTo(o);
		else 
			return pid - o.pid;
	}
}
