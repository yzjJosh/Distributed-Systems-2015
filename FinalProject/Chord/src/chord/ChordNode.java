package chord;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

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

	protected ChordNode predecessor = this;
	protected HashMap<Long, Integer> listOfLinks = new HashMap<Long, Integer>(); // The list of the ids of links.
	private static final int MAX_RESPONSE_TIME = 5000; // The maximum response time of this system.
	private static final HashMap<Long, String> clusterInfo = new HashMap<Long, String>(); // information  about other node.
	private static final ArrayList<Long> ids = new ArrayList<Long>();  //store all the hashes of other nodes
	private static HashMap<Serializable, Serializable> data = new HashMap<Serializable, Serializable>(); // The data stored in the chord node.

	protected transient List<Socket> socket = new LinkedList<Socket>();
	protected static transient CommunicationManager manager = new CommunicationManager();

	public void initConnection(String path) throws IOException,
			FileNotFoundException {

		String serverInfo;
		BufferedReader reader = new BufferedReader(new FileReader(
				new File(path)));
		while ((serverInfo = reader.readLine()) != null) {
			// Split the serverInfo to get the host and port.
			String[] splits = serverInfo.split(" ");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);
			long hash = Long.parseLong(splits[2]);
			clusterInfo.put(hash, serverInfo);
			ids.add(hash);

		}
		reader.close();
		// Write new node info into the file.
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path), true));
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
		System.out.println("fingerTable is set!");
		int num = clusterInfo.size();

		if (num > 0) {
			int index = (int) Math.random() * num;
			String[] splits = clusterInfo.get(ids.get(index)).split(" ");
			String connect_ip = splits[0];
			int connect_port = Integer.parseInt(splits[1]);
			// Connect to an exsiting node
			int id_link = manager.connect(connect_ip, connect_port);
			listOfLinks.put(ids.get(index), id_link); // Bind the chord id with a link num.
			manager.setOnMessageReceivedListener(id_link, new GenericMessageListener(this));
			manager.sendMessage(id_link, new Message().put("MessageType", "LinkSetup").put("ClientID", identifier.getID()));
			try {
				manager.sendMessageForResponse(id_link,
						new Message().put("MessageType", "RequestJoin"),
						new MessageFilter() {

							@Override
							public boolean filter(Message msg) {
								if (msg != null) {
									if (msg.containsKey("Reply")) {
										return true;
									}
								}
								return false;
							}
						}, MAX_RESPONSE_TIME, new JoinMessageListener(this),
						false);
				
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

//		System.out.println("Before find predecessor!");
		final LinkedList<ChordNode> result = new LinkedList<ChordNode>();
		while (!id.isBetween(this.getChordID(), successor.getChordID()) && !id.equals(successor.getChordID())) {
			long m_id = m.identifier.getID();
			if (m_id != this.identifier.getID()) {
				if (clusterInfo.containsKey(m_id)) {
					// If the link has been set up.
					if (listOfLinks.containsKey(m_id)) {
						try {
							manager.sendMessageForResponse(listOfLinks.get(m_id), new Message().put("MessageType", "FindPredecessor").put("ID", id), new MessageFilter() {
								@Override
								public boolean filter(Message msg) {		
									if (msg != null) {
										if (msg.containsKey("NextNode")) {
											return true;
										}
									}
									return false;
								}
							}, MAX_RESPONSE_TIME,  new OnMessageReceivedListener() {
								@Override
								public void OnMessageReceived(CommunicationManager manager,
										int id, Message msg) {
									result.add( (ChordNode) msg.get("NextNode") );
								}
								@Override
								public void OnReceiveError(CommunicationManager manager,
										int id) {
									System.err.println("Find predecessor received error!");
								} }, true);
						} catch (IOException e) {
							e.printStackTrace();
						}
						m = result.peek();
					} else {
						String[] splits = clusterInfo.get(m_id).split(" ");
						String connect_ip = splits[0];
						int connect_port = Integer.parseInt(splits[1]);
						
						try {
							int id_link = manager.connect(connect_ip, connect_port);
							listOfLinks.put(m_id, id_link); // Bind the chord id with
							manager.sendMessageForResponse(listOfLinks.get(m_id), new Message().put("MessageType", "FindPredecessor").put("ID", id), new MessageFilter() {
								@Override
								public boolean filter(Message msg) {		
									if (msg != null) {
										if (msg.containsKey("NextNode")) {
											return true;
										}
									}
									return false;
								}
							}, MAX_RESPONSE_TIME,  new OnMessageReceivedListener() {
								@Override
								public void OnMessageReceived(CommunicationManager manager,
										int id, Message msg) {
									result.add( (ChordNode) msg.get("NextNode") );
								}
								@Override
								public void OnReceiveError(CommunicationManager manager,
										int id) {
									System.err.println("Find predecessor received error!");
								} }, true);
							
						} catch (IOException e) {
							e.printStackTrace();
						}
						m = result.peek();
						}
				}
				
			} else {
				m = m.closest_preceding_finger(id);

			}
		}
//		System.out.println("After find predecessor!");
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
//		System.out.println("Before join, the successor is " + successor.getChordID().getID());
		successor = node.find_successor(this.getChordID());
//		System.out.println("After join, the successor is " + successor.getChordID().getID());
	}

	/**
	 * Verifies the successor, and tells the successor about this node. Should
	 * be called periodically.
	 */
	public void stabilize() {
		ChordNode node = successor.getPredecessor();
		if (node != null) {
			ChordID key = node.getChordID();
			if ((this == successor) || key.isBetween(this.getChordID(), successor.getChordID())) {
				successor = node;
			}
		}
		long successorID = successor.getChordID().getID();
		if (clusterInfo.containsKey(successorID)) {
			// If the link has been set up.
			if (listOfLinks.containsKey(successorID)) {
				
					try {
						manager.sendMessage(listOfLinks.get(successorID), new Message().put("MessageType", "Notify").put("Notifier", ChordNode.this));
					} catch (IOException e) {
						e.printStackTrace();
					}
					
			} else {
			String[] splits = clusterInfo.get(successorID).split(" ");
			String connect_ip = splits[0];
			int connect_port = Integer.parseInt(splits[1]);
			
			try {
				int id_link = manager.connect(connect_ip, connect_port);
				listOfLinks.put(successorID, id_link); // Bind the chord id with
				manager.setOnMessageReceivedListener(id_link,
						new GenericMessageListener(this));
				manager.sendMessage(id_link, new Message().put("MessageType", "LinkSetup").put("ClientID", identifier.getID()));
				manager.sendMessage(id_link, new Message().put("MessageType", "Notify").put("Notifier", ChordNode.this));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
		}

	}

	public void notifyPredecessor(ChordNode node) {
		ChordID key = node.getChordID();
		System.out.println("Start notify process!");
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
			ChordNode node = find_successor(key);
			finger.setNode(node);
		}
	}

	public boolean storeData(Serializable key, Serializable value) {
		ChordID transformKey = new ChordID((String) key);
		if (transformKey.equals(this.identifier)) {
			data.put(key, value);
			return true;
		} else {
			int i = 0;
			FingerTableEntry finger = null;
			for (; i < 32; i++) {
				finger = fingerTable.getFinger(i);
				if (transformKey.isBetween(finger.interval[0],
						finger.interval[1])) {
					break;
				}
			}
			long successorID = finger.node.getChordID().getID();
			final LinkedList<Boolean> result = new LinkedList<Boolean>();
			if (clusterInfo.containsKey(successorID)) {
				// If the link has been set up.
				if (listOfLinks.containsKey(successorID)) {
					try {
						manager.sendMessageForResponse(
								listOfLinks.get(successorID),
								new Message().put("MessageType", "StoreData")
										.put("Key", key).put("Value", value),
								new MessageFilter() {

									@Override
									public boolean filter(Message msg) {
										if (msg != null) {
											if (msg.containsKey("StoreSuccess")) {
												return true;
											}
										}
										return false;
									}
								}, MAX_RESPONSE_TIME,
								new OnMessageReceivedListener() {

									@Override
									public void OnMessageReceived(
											CommunicationManager manager,
											int id, Message msg) {
										System.out
												.println("Store reqeust has been sent to the next node!");
										result.add(true);
									}

									@Override
									public void OnReceiveError(
											CommunicationManager manager, int id) {
										System.err
												.println("On reveived error in the storedata function!");
										result.add(false);
									}

								}, false);

					} catch (IOException e) {
						e.printStackTrace();
						result.add(false);
					}

					if (result.peek()) {
						return true;
					} else {
						return false;
					}

				} else {
					String[] splits = clusterInfo.get(successorID).split(" ");
					String connect_ip = splits[0];
					int connect_port = Integer.parseInt(splits[1]);

					try {
						int id_link = manager.connect(connect_ip, connect_port);
						listOfLinks.put(successorID, id_link); // Bind the chord id with
						manager.setOnMessageReceivedListener(id_link,
								new GenericMessageListener(ChordNode.this));
						manager.sendMessage(id_link, new Message().put("MessageType", "LinkSetup").put("ClientID", identifier.getID()));
						manager.sendMessageForResponse(
								listOfLinks.get(successorID),
								new Message().put("MessageType", "StoreData")
										.put("Key", key).put("Value", value),
								new MessageFilter() {

									@Override
									public boolean filter(Message msg) {
										if (msg != null) {
											if (msg.containsKey("StoreSuccess")) {
												return true;
											}
										}
										return false;
									}
								}, MAX_RESPONSE_TIME,
								new OnMessageReceivedListener() {

									@Override
									public void OnMessageReceived(
											CommunicationManager manager,
											int id, Message msg) {
										System.out
												.println("Store reqeust has been sent to the next node!");
										result.add(true);
									}

									@Override
									public void OnReceiveError(
											CommunicationManager manager, int id) {
										System.err
												.println("On reveived error in the storedata function!");
										result.add(false);
									}

								}, false);
						

					} catch (IOException e) {
						e.printStackTrace();
						result.add(false);
					}
				}
				if (result.peek()) {
					return true;
				} else {
					return false;
				}
			}
			data.put(key, value);
			return true;
		}
	}

	// Look up the key and return the value.
	public Serializable getValue(Serializable key) {
		if (data.containsKey(key)) {
			return data.get(key);
		} else {
			// Look up the next node in the finger table.
			int i = 0;
			FingerTableEntry finger = null;
			ChordID transformKey = new ChordID((String) key);
			for (; i < 32; i++) {
				finger = fingerTable.getFinger(i);
				if (transformKey.isBetween(finger.interval[0],
						finger.interval[1])) {
					break;
				}
			}
			long successorID = finger.node.getChordID().getID();
			final LinkedList<Serializable> result = new LinkedList<Serializable>(); // STOTE THE RETURN DATA
			System.out.println(successorID);
			if (clusterInfo.containsKey(successorID)) {
				// If the link has been set up.
				if (listOfLinks.containsKey(successorID)) {
					try {
						manager.sendMessageForResponse(
								listOfLinks.get(successorID),
								new Message()
										.put("MessageType", "RetrieveData")
										.put("Key", key),
								new MessageFilter() {

									@Override
									public boolean filter(Message msg) {
										if (msg != null) {
											if (msg.containsKey("RetrieveReply")) {
												return true;
											}
										}
										return false;
									}
								}, MAX_RESPONSE_TIME,
								new OnMessageReceivedListener() {

									@Override
									public void OnMessageReceived(
											CommunicationManager manager,
											int id, Message msg) {
										System.out
												.println("Oh, yeah!! The corresponding value is " + msg.get("RetrieveReply"));
										result.add(msg.get("RetrieveReply"));
									}

									@Override
									public void OnReceiveError(
											CommunicationManager manager, int id) {
										System.err
												.println("On reveived error in the getValue function!");
										result.add(null);
									}

								}, false);

					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					String[] splits = clusterInfo.get(successorID).split(" ");
					String connect_ip = splits[0];
					int connect_port = Integer.parseInt(splits[1]);

					// Connect to an exsiting node
					try {
						int id_link = manager.connect(connect_ip, connect_port);
						listOfLinks.put(successorID, id_link); // Bind the chord id with
						manager.setOnMessageReceivedListener(id_link,
								new GenericMessageListener(ChordNode.this));
						manager.sendMessage(id_link, new Message().put("MessageType", "LinkSetup").put("ClientID", identifier.getID()));
						manager.sendMessageForResponse(
								id_link,
								new Message()
										.put("MessageType", "RetrieveData")
										.put("Key", key),
								new MessageFilter() {

									@Override
									public boolean filter(Message msg) {
										if (msg != null) {
											if (msg.containsKey("RetrieveReply")) {
												return true;
											}
										}
										return false;
									}
								}, MAX_RESPONSE_TIME,
								new OnMessageReceivedListener() {

									@Override
									public void OnMessageReceived(
											CommunicationManager manager,
											int id, Message msg) {
										System.out
												.println("Oh, yeah!! The corresponding value is " + msg.get("RetrieveReply"));
										result.add(msg.get("RetrieveReply"));
									}

									@Override
									public void OnReceiveError(
											CommunicationManager manager, int id) {
										System.err
												.println("On reveived error in the getValue function!");
										result.add(null);
									}

								}, false);
						

					} catch (IOException e) {
						result.add(null);
						System.err.println("IO exception 1");
					}
				}
				return result.peek();

			}
			return data.get(key);
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
		final ChordNode node = new ChordNode();
		final String file = args[0];
		
		try {
			node.initConnection(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		new Thread() {
			@Override
			public void run() {
				while (true) {
				ChordNode preceding = node.getSuccessor().getPredecessor();
		
				// stablize
				node.stabilize();
				if (preceding == null) {
					node.getSuccessor().stabilize();
				} else {
					preceding.stabilize();
				}
				// fix fingertable
				node.fixFingers();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			}
		}.start();
		new Thread() {
			@Override
			public void run() {	
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				while (true) {
					
					try {
					System.out.println("Do you want to 1. store data or 2. retrieve data or 3. print finger table or 4. print successor and predecessor?");
					String option = br.readLine();
					if (option.equals("1")) {
						System.out.println("Please input the key and value!");
						String data = br.readLine();
						String key = data.split(" ")[0];
						String value = data.split(" ")[1];
						node.storeData(key, value);
					} else if (option.equals("2")) {
						System.out.println("Please input the key for retrival!");
						String key = br.readLine();
						System.out.println("The value is " + node.getValue(key));
						
					} else if (option.equals("3")) {
						node.fingerTable.print();
						
					}  else if (option.equals("4")) {
						String successor = node.successor == null ? "null" : node.successor.getChordID().getID() + "";
						String predecessor = node.predecessor == null ? "null" : node.predecessor.getChordID().getID() + "";
						System.out.println("Successor: " + successor  + " | Predecessor: " + predecessor);
						
					} else {
						System.out.println("Input is illegal!!");
					}
					
					} catch (IOException e) {
						System.out.println("IO Error");
					}
				}
			}
		}.start();
	}

}
