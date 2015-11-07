package communication;

/**
 * OnConnectionListener listens to connection events
 * @author Josh
 *
 */
public interface OnConnectionListener {
	
	/**
	 * Called when a connection is established
	 * @param manager the communication manager which this connection belongs to
	 * @param id the id (unique in each communication manager) of this connection
	 */
	public void OnConnected(final CommunicationManager manager, final int id);
	
	/**
	 * Called when fails to establish a connection
	 * @param manager the communication manager which performs this connection
	 */
	public void OnConnectFail(final CommunicationManager manager);
	
}
