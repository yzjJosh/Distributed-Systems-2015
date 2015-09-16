package server;

import java.io.Serializable;

/**
 * Message is the object transfered between processes.
 *
 */
enum MessageType {
	TYPE_CLOCK_MESSAGE,//The message used to update clock between processes.
	TYPE_CS_REQUEST_READ,	//The message used to request a critial section for reading.
	TYPE_CS_REQUEST_WRITE ,//The message used to request a critial section for writing.
	TYPE_ACKNOWLEDGE ,	//The message used to respond to a cs request.
	TYPE_CS_RELEASE,	//The message used to release a critical section.
	TYPE_RESERVE_SEATE,	//The message used for a client to request the server to reserve seates.
	TYPE_SEARCH_SEATE,	//The message used for a client to request the server to search seates reserved by a name.
	TYPE_DELETE_SEATE ,	//The message used for a client to request the server to release seates reserved by a name.
	TYPE_RESPOND_TO_CLIENT//The message used for server to respond to a client.
}
public class Message implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public static MessageType type; 	//The type of this message. Different types are defined above.
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
}
