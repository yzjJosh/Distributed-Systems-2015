package chord;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;

/**
 * ChordNodeListener, can be seen as the server-end listener
 * @author Yu Sun
 */
public class ChordNodeListener implements OnConnectionListener {
	ChordNode node;
	ChordNodeListener(ChordNode node) {
		this.node = node;
	}
	
	@Override
	public void OnConnected(CommunicationManager manager, int id) {   
			manager.setOnMessageReceivedListener(id, new GenericMessageListener(node));
			System.out.println("Link " + id + " is connected!!!");	
	}

	@Override
	public void OnConnectFail(CommunicationManager manager) {
		System.err.println("Connection dFailed!");
		
	}

}
