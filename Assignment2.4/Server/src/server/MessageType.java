package server;

/**
 * Type of a message
 *
 */
enum MessageType {
	CLOCK_MESSAGE,			//The message used to update clock between processes.
	CS_REQUEST_READ,		//The message used to request a critial section for reading.
	CS_REQUEST_WRITE ,		//The message used to request a critial section for writing.
	ACKNOWLEDGE_READ ,		//The message used to respond to a cs request read.
	ACKNOWLEDGE_WRITE,		//The message used to resoind to a cs request write.
	CS_RELEASE,				//The message used to release a critical section.
	RESERVE_SEATE,			//The message used for a client to request the server to reserve seates.
	SEARCH_SEATE,			//The message used for a client to request the server to search seates reserved by a name.
	DELETE_SEATE ,			//The message used for a client to request the server to release seates reserved by a name.
	RESPOND_TO_CLIENT,		//The message used for server to respond to a client.
	SERVER_SYNC				//The message used for server to sychronize seate information with other servers.			
}