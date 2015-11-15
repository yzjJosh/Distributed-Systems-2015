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
			if (msg.get("MessageType").equals("RequestJoin")) {
				try {
					System.out.println("Receive request of join!");
					manager.sendMessage(id, new Message().put("Reply", node));
				} catch (IOException e) {
					e.printStackTrace();
				}						
			} else if(msg.get("MessageType").equals("Notify")) {
				System.out.println("Received notification!");
				node.notifyPredecessor((ChordNode) (msg.get("Notifier")));
				
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
			} else if(msg.get("MessageType").equals("LinkSetup")) {
				Serializable clientID = msg.get("ClientID");
				System.out.println("Add a client : " + clientID + "to listOflinks! ");
				node.listOfLinks.put((Long)clientID, id);
			} else if(msg.get("MessageType").equals("FindPredecessor")) {
				ChordID chord_id = (ChordID) msg.get("ID");
				ChordNode temp = node.closest_preceding_finger(chord_id);
				try {
					manager.sendMessage(id, new Message().put("NextNode",temp));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	@Override
	public void OnReceiveError(CommunicationManager manager, int id) {
		System.err.println("Received error!");
		
	}
	

}
