package chord;

import communication.CommunicationManager;
import communication.OnConnectionListener;

/**
 * ChordNodeListener, can be seen as the server-end listener
 * @author Yu Sun
 */
public class ChordNodeListener implements OnConnectionListener {
	
	@Override
	public void OnConnected(CommunicationManager manager, int id) {   
			System.out.println("Link " + id + " is connected!!!");	
	}

	@Override
	public void OnConnectFail(CommunicationManager manager) {
		System.err.println("Connection dFailed!");
		
	}

}
