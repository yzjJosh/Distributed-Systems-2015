package chord;

import java.io.IOException;

import communication.CommunicationManager;
import communication.Message;
import communication.OnMessageReceivedListener;

/**
 * GenericMessageListener
 * @author Yu Sun
 */
public class GenericMessageListener implements OnMessageReceivedListener{
	ChordNode node = null;
	GenericMessageListener(ChordNode node) {
		this.node = node;
	}
	@Override
	public void OnMessageReceived(CommunicationManager manager, int id,
			Message msg) {
		if (msg.containsKey("MessageType" )){
			if(msg.get("MessageType").equals("NotifyPredecessor")) {
				try {
					manager.sendMessage(id, new Message().put("Reply", node));
				} catch (IOException e) {
					System.err.println("Reply notify predecessor error!");
				}
			} else if(msg.get("MessageType").equals("StoreData")) {
				boolean storeFlag = node.storeData(msg.get("Key"), msg.get("Value"));
				if (storeFlag == true) {
					try {
						manager.sendMessage(id, new Message().put("StoreSuccess", ""));
					} catch (IOException e) {
						System.err.println("Reply store error!");
					}
				} 
			}
		}
		
	}
	
	@Override
	public void OnReceiveError(CommunicationManager manager, int id) {
		System.err.println("Received error!");
		
	}
	

}
