package message;

/**
 * Type of a message
 *
 */
public enum MessageType {
	CLOCK_MESSAGE,			//The message used to update clock between processes.
	CS_REQUEST_READ,		//The message used to request a critical section for reading.
	CS_REQUEST_WRITE ,		//The message used to request a critical section for writing.
	ACKNOWLEDGE_READ ,		//The message used to respond to a cs request read.
	ACKNOWLEDGE_WRITE,		//The message used to respond to a cs request write.
	CS_RELEASE,				//The message used to release a critical section.
	RESERVE_SEAT,			//The message used for a client to request the server to reserve seats.
	SEARCH_SEAT,			//The message used for a client to request the server to search seats reserved by a name.
	DELETE_SEAT ,			//The message used for a client to request the server to release seats reserved by a name.
	RESPOND_TO_CLIENT,		//The message used for server to respond to a client.
	SERVER_SYNC,			//The message used for server to sychronize seats information with other servers.
	SERVER_SYNC_RESPONSE,	//The message used for server to respond to a SERVER_SYNC
	SERVER_SYNC_COMPLETE	//The message used for server to comfirm that sychronization is completed
}