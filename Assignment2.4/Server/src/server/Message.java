package server;

import java.io.Serializable;

/**
 * Message is the object transfered between processes.
 *
 */
public class Message implements Serializable, Comparable<Message> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Type of a message
	 *
	 */
	enum TYPE{
		CLOCK_MESSAGE,		//The message used to update clock between processes.
		CS_REQUEST_READ,	//The message used to request reading of critial section.
		CS_REQUEST_WRITE,	//The message used to request writing of critial section.
		ACKNOWLEDGE,		//The message used to respond to a cs request.
		CS_RELEASE,			//The message used to release a critical section.
		RESERVE_SEATE,		//The message used for a client to request the server to reserve seates.
		SEARCH_SEATE,		//The message used for a client to request the server to search seates reserved by a name.
		DELETE_SEATE,		//The message used for a client to request the server to release seates reserved by a name.
		RESPOND_TO_CLIENT,	//The message used for server to respond to a client.
		SERVER_INIT			//The message used for server for initialization.
	}
	
	public final TYPE type; 					//The type of this message. Different types are defined above.
	public final int pid;					//The pid of sender.
	public final Serializable content;		//The content of this message.
	
	/**
	 * Create a new message.
	 * @param type The type of message.
	 * @param content The content.
	 * @param pid	The sender's pid.
	 */
	public Message(TYPE type, Serializable content, int pid){
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
