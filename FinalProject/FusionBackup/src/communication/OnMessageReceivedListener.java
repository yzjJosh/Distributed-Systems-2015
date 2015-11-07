package communication;

/**
 * OnMessageReceivedListener listens to message receiving events
 * @author Josh
 *
 */
public interface OnMessageReceivedListener {

	/**
	 * Called when a message is received from a socket
	 * @param socket the socket
	 * @param id the id of this connection
	 * @param msg the received message
	 */
	public void OnMessageReceived(final CommunicationManager manager, final int id, final Message msg);
	
	/**
	 * Called when error occurs on receiving message
	 * @param manager the manager which manages this connection
	 * @param id the communication id
	 */
	public void OnReceiveError(final CommunicationManager manager, final int id);
	
}
