package chord;

import communication.CommunicationManager;
import communication.Message;
import communication.OnMessageReceivedListener;
/**
 * NotifyNodeListener
 * @author Yu Sun
 */
public class NotifyMessageListener implements OnMessageReceivedListener {
	ChordNode node;
	NotifyMessageListener(ChordNode node) {
		this.node = node;
	}
	@Override
	public void OnMessageReceived(CommunicationManager manager, int id,
			Message msg) {
		node.notifyPredecessor((ChordNode) msg.get("Reply"));
		
		
	}

	@Override
	public void OnReceiveError(CommunicationManager manager, int id) {
		System.err.println("Receipt of Message Failed!");
		
	}

}