package server;

import java.io.Serializable;

/**
 * Message is the object transfered between processes.
 *
 */

public class Message implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final int TYPE_CLOCK_MESSAGE = 0; //The message used to update clock between processes.
	public static final int TYPE_CS_REQUEST_READ= 1;	//The message used to request a critial section for reading.
	public static final int TYPE_CS_REQUEST_WRITE = 2;//The message used to request a critial section for writing.
	public static final int TYPE_ACKNOWLEDGE = 3;	//The message used to respond to a cs request.
	public static final int TYPE_CS_RELEASE = 4;	//The message used to release a critical section.
	public static final int TYPE_RESERVE_SEATE = 5;	//The message used for a client to request the server to reserve seates.
	public static final int TYPE_SEARCH_SEATE = 6;	//The message used for a client to request the server to search seates reserved by a name.
	public static final int TYPE_DELETE_SEATE = 7;	//The message used for a client to request the server to release seates reserved by a name.
	public static final int TYPE_RESPOND_TO_CLIENT = 8;//The message used for server to respond to a client.
	
	public final int type; 					//The type of this message. Different types are defined above.
	public final int pid;					//The pid of sender.
	public final Serializable content;		//The content of this message.
	
	/**
	 * Create a new message.
	 * @param type The type of message.
	 * @param content The content.
	 * @param pid	The sender's pid.
	 */
	public Message(int type, Serializable content, int pid){
		this.type = type;
		this.content = content;
		this.pid = pid;
	}
}
