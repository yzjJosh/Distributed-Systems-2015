package chord;

import java.io.IOException;

import communication.CommunicationManager;
import communication.Message;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;
/**
 * ChordNodeListener, can be seen as the server-end listener
 * @author Yu Sun
 */
public class ChordNodeListener implements OnConnectionListener {
	ChordNode node = null;
	ChordNodeListener(ChordNode node) {
		this.node = node;
	}
	@Override
	public void OnConnected(CommunicationManager manager, int id) {
		
			manager.setOnMessageReceivedListener(id, new OnMessageReceivedListener() {

				@Override
				public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
					if (msg.get("MessageType").equals("RequestJoin")) {
						try {
							manager.sendMessage(id, new Message().put("Reply", node));
						} catch (IOException e) {
							e.printStackTrace();
						}						
					}
				}

				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					System.err.println("Message Received Failed!");
				}
				
			});
			System.out.println("Link " + id + " is connected!!!");	
	}

	@Override
	public void OnConnectFail(CommunicationManager manager) {
		System.err.println("Connection dFailed!");
		
	}

}
