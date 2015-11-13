package chord;

import communication.CommunicationManager;
import communication.Message;
import communication.OnMessageReceivedListener;
/**
 * JoinNodeListener, can be seen as the client-end listener
 * @author Yu Sun
 */
public class JoinMessageListener implements OnMessageReceivedListener {
	ChordNode node;
	JoinMessageListener(ChordNode node) {
		this.node = node;
	}
	@Override
	public void OnMessageReceived(CommunicationManager manager, int id,
			Message msg) {
		System.out.println("Ready to join..");
		node.join((ChordNode) msg.get("Reply"));
		
		
	}

	@Override
	public void OnReceiveError(CommunicationManager manager, int id) {
		System.err.println("Receipt of Message Failed!");
		
	}

}
