package chord;

import java.io.IOException;
import java.io.Serializable;

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
		if (msg.containsKey("MessageType" )) {
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
			} else if(msg.get("MessageType").equals("RetrieveData")) {
				Serializable storeValue= node.getValue(msg.get("Key"));
				if (storeValue != null) {
					try {
						manager.sendMessage(id, new Message().put("RetrieveReply", storeValue));
					} catch (IOException e) {
						System.err.println("No the corresponding value!");
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
