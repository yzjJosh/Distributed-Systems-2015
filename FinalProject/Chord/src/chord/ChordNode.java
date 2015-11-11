package chord;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;

/**
 * Provides all methods necessary for a user application. This includes methods
 * for changing connectivity to the network (create, join, leave) as well as for
 * working with content (insert, retrieve, remove).
 * 
 * @author Yu Sun
 */
public class ChordNode implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String ip;
	int port;
	/**
	 * The identifier of this node
	 */
	ChordID identifier;
	/**
	 * The node's finger table.
	 */
	protected FingerTable fingerTable;
	/**
	 * A reference to this node's successor.
	 */
	protected ChordNode successor = this;
	/**
	 * A reference to this node's predecessor.
	 */
	protected ChordNode predecessor = null;
	protected HashMap <Integer, Integer>listOfLinks = new HashMap<Integer, Integer>(); // The list of the ids of links.
	private static final int MAX_RESPONSE_TIME = 5000; // The maximum response time of this system.
	private static final HashMap<Integer, String> clusterInfo = new HashMap<Integer, String>(); // information about other node.
	private static final ArrayList<Integer> ids = new ArrayList<Integer>(); 
															
	protected transient List<Socket> socket = new LinkedList<Socket>();
	protected static transient CommunicationManager manager = new CommunicationManager();

	public void initConnection(String path) throws IOException, FileNotFoundException {

		String serverInfo;
		BufferedReader reader = new BufferedReader(new FileReader(
				new File(path)));
		while ((serverInfo = reader.readLine()) != null) {
			// Split the serverInfo to get the host and port.
			String[] splits = serverInfo.split(" ");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);
			int hash = Integer.parseInt(splits[2]);
			clusterInfo.put(hash, serverInfo);
			ids.add(hash);

		}
		reader.close();
		// Write new node info into the file
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				new File(path)));

		// Add itself to the information sheet.

		// Specify a random port number.
		port = (int) (10000 + Math.random() * 10000);
		ip = InetAddress.getLocalHost().getHostAddress();
		identifier = new ChordID(port + ip);
		String data = ip + " " + port + " " + identifier.getID() + "\n";
		System.out.println(data);
		writer.write(data);
		writer.close();
		 fingerTable = new FingerTable(this);
	    
		int num = clusterInfo.size();
		if (num > 0) {
			int index = (int) Math.random() * num;
			String[] splits = clusterInfo.get(ids.get(index)).split(" ");
			String connect_ip = splits[0];
			int connect_port = Integer.parseInt(splits[1]);
			// Connect to an exsiting node
			int id_link = manager.connect(connect_ip, connect_port);
			listOfLinks.put(ids.get(index), id_link); //Bind the chord id with a link num 
			manager.setOnMessageReceivedListener(id_link, new OnMessageReceivedListener() {
				
				@Override
				public void OnMessageReceived(CommunicationManager manager, int id,
						Message msg) {
					if (msg.containsKey("MessageType" )){
						if(msg.get("MessageType").equals("NotifyPredecessor")) {
							try {
								manager.sendMessage(id, new Message().put("Reply", ChordNode.this));
							} catch (IOException e) {
								System.err.println("Reply notify predecessor error!");
							}
						}
					}
					
				}
				
				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					System.err.println("Received error!");
				}
				
			});
			try {
				manager.sendMessageForResponse(id_link, new Message().put("MessageType", "Join"), new MessageFilter() {
					
					@Override
					public boolean filter(Message msg) {
						if (msg != null) {
							if (msg.containsKey("Reply")) {
								return true;
							}
						}
						return false;
					}		
				}, MAX_RESPONSE_TIME,  new JoinMessageListener(this), false);
			} catch (IOException e) {
				System.err.println(e);
			}
		}
		manager.waitForConnection(port, new ChordNodeListener(this));

	}
	
	/**
	 * Find the successor node for the given identifier. The successor node is
	 * the node responsible for storing any <key, value> pair whose key hashes
	 * to the identifier being sought.
	 * 
	 * @param id
	 *            The identifier being sought
	 * 
	 * @return The node responsible for id
	 */
	public ChordNode find_successor(ChordID id) {
		ChordNode m = find_predecessor(id);
		return m.successor;
	}

	/*
	 * ask node to find id's precessor
	 */
	public ChordNode find_predecessor(ChordID id) {
		ChordNode m = this;
		while (!id.isBetween(this.getChordID(), successor.getChordID())
				&& !id.equals(successor.getChordID())) {
			m = m.closest_preceding_finger(id);
		}
		return m;
	}

	public ChordNode closest_preceding_finger(ChordID id) {

		for (int i = 31; i >= 0; i--) {
			FingerTableEntry finger = fingerTable.getFinger(i);
			ChordID fingerID = finger.getNode().getChordID();
			if (fingerID.isBetween(this.getChordID(), id)) {
				return finger.getNode();
			}
		}
		return this;

	}

	/**
	 * Joins a Chord ring with a node in the Chord ring
	 * 
	 * @param node
	 *            a bootstrapping node
	 */
	public void join(ChordNode node) {
		predecessor = null;
		successor = node.find_successor(this.getChordID());
	}

	/**
	 * Verifies the successor, and tells the successor about this node. Should
	 * be called periodically.
	 */
	public void stabilize() {
		ChordNode node = successor.getPredecessor();
		if (node != null) {
			ChordID key = node.getChordID();
			if ((this == successor)
					|| key.isBetween(this.getChordID(), successor.getChordID())) {
				successor = node;
			}
		}
		//If the link has been set up.
		int successorID = successor.getChordID().getID();
		if (clusterInfo.containsKey(successorID)) {
		if (listOfLinks.containsKey(successorID) ) {
			try{
			manager.sendMessageForResponse(listOfLinks.get(successorID), new Message().put("MessageType", "NotifyPredecessor"), new MessageFilter() {
				
				@Override
				public boolean filter(Message msg) {
					if (msg != null) {
						if (msg.containsKey("Reply")) {
							return true;
						}
					}
					return false;
				}		
			}, MAX_RESPONSE_TIME,  new NotifyMessageListener(this), false);
					
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
			String[] splits = clusterInfo.get(successorID).split(" ");
			String connect_ip = splits[0];
			int connect_port = Integer.parseInt(splits[1]);
		
		// Connect to an exsiting node
		try {
			int id_link = manager.connect(connect_ip, connect_port);
			manager.setOnMessageReceivedListener(id_link, new OnMessageReceivedListener() {
				
				@Override
				public void OnMessageReceived(CommunicationManager manager, int id,
						Message msg) {
					if (msg.containsKey("MessageType" )){
						if(msg.get("MessageType").equals("NotifyPredecessor")) {
							try {
								manager.sendMessage(id, new Message().put("Reply", ChordNode.this));
							} catch (IOException e) {
								System.err.println("Reply notify predecessor error!");
							}
						}
					}
					
				}
				
				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					System.err.println("Received error!");
					
				}
				
			});
			
			manager.sendMessageForResponse(id_link, new Message().put("MessageType", "NotifyPredecessor"), new MessageFilter() {
				
				@Override
				public boolean filter(Message msg) {
					if (msg != null) {
						if (msg.containsKey("Reply")) {
							return true;
						}
					}
					return false;
				}		
			}, MAX_RESPONSE_TIME,  new NotifyMessageListener(this), false);
					
		} catch (IOException e) {
			e.printStackTrace();
		}
		}
	

	}

	public void notifyPredecessor(ChordNode node) {
		ChordID key = node.getChordID();
		if (predecessor == null
				|| key.isBetween(predecessor.getChordID(), this.getChordID())) {
			predecessor = node;
		}
	}

	/**
	 * Refreshes finger table entries.
	 */
	public void fixFingers() {
		for (int i = 0; i < 32; i++) {
			FingerTableEntry finger = fingerTable.getFinger(i);
			ChordID key = finger.getStart();
			System.out.println(key.getID());
			ChordNode node = find_successor(key);
			finger.setNode(node);
			finger.setInterval(key, node.getChordID());
		}
	}

	public ChordID getChordID() {
		return identifier;
	}

	public ChordNode getPredecessor() {
		return predecessor;
	}

	public ChordNode getSuccessor() {
		return successor;
	}
	
	public static void main(String[] args) {
		ChordNode node = new ChordNode();
	    try {
			node.initConnection(args[0]);
			//stablize
			ChordNode preceding = node.getSuccessor().getPredecessor();
			node.stabilize();
			if (preceding == null) {
				node.getSuccessor().stabilize();
			} else {
				preceding.stabilize();
			}
			//fix fingertable
			node.fixFingers();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
