package constants;

public enum MessageType{
	CONNECT_REQUEST,
	CONNECT_ACCEPTED,
	RECOVER_REQUEST,
	RECOVER_RESULT,
	PAUSE_UPDATE,
	UPDATE_PAUSED,
	RESUME_UPDATE,
	DATA_REQUEST,
	DATA_RESPONSE,
	BACKUP_UPDATE,
	SIGNAL,
	SIGNAL_ACK,
	EXCEPTION
}