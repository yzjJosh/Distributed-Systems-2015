package chord;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;
import constants.MessageType;
import exceptions.OperationFailsException;

public class ChordNode {
	
	private final CommunicationManager manager;
	private final long id;
	private final ConcurrentHashMap<Integer, Long> link2id;
	private final ConcurrentHashMap<Long, Integer> id2link;
	private final FingerTable fingerTable;
	private long predecessor;
	private long successor;
	
	public ChordNode(Map<Integer, String> hosts, int index){
		if(hosts == null || !hosts.containsKey(index) || hosts.get(index) == null)
			throw new IllegalArgumentException();
		manager = new CommunicationManager();
		link2id = new ConcurrentHashMap<Integer, Long>();
		id2link = new ConcurrentHashMap<Long, Integer>();
		id = hash(hosts.get(index));
		predecessor = id;
		successor = id;
		fingerTable = new FingerTable(id);
		LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
		for(final Map.Entry<Integer, String> entry: hosts.entrySet()){
			if(entry.getKey() == index) continue;
			String[] temp = entry.getValue().split(":");
			final String ip = temp[0];
			final int port = Integer.parseInt(temp[1]);
			final Semaphore s = new Semaphore(0);
			semaphores.add(s);
			new Thread(){
				@Override
				public void run(){
					int link  = -1;
					try {
						link = manager.connect(ip, port);
						manager.sendMessageForResponse(link, 
													   new Message().put("MessageType", MessageType.CONNECTION_REQUEST).
																	 put("id", id),
													   new MessageFilter(){
															@Override
															public boolean filter(Message msg) {
																return msg!=null && msg.containsKey("MessageType")
																		&& msg.get("MessageType")==MessageType.CONNECTION_ACCEPTED;
															}
														},
														5000,
														new OnMessageReceivedListener(){
															@Override
															public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
																long nodeId = hash(entry.getValue());
																link2id.put(id, nodeId);
																id2link.put(nodeId, id);
																manager.setOnMessageReceivedListener(id, new MessageListener());
																System.out.println("Connected to chord node "+nodeId);
															}
															@Override
															public void OnReceiveError(CommunicationManager manager, int id) {
																manager.closeConnection(id);
															}
														}, true);
						
						
					} catch (IOException e) {
						if(link >= 0)
							manager.closeConnection(link);
					}
					s.release();
				}
			}.start();
		}
		for(Semaphore s : semaphores)
			try {
				s.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		int port = Integer.parseInt(hosts.get(index).split(":")[1]);
		manager.waitForConnection(port, new ConnectionListener());
		System.out.println("ChordNode starts, id="+id);
		System.out.println("Waiting for connection at port "+port);
	}
	
	private long hash(String str){
		return str.hashCode() & 0x00000000ffffffffL;
	}
	
	private long find_successor(long target) throws OperationFailsException{
		long predecessor = find_predecessor(target);
		if(predecessor == id) return successor;
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			manager.sendMessageForResponse(id2link.get(predecessor), new Message().put("MessageType", MessageType.FIND_SUCCESSOR), 
											new MessageFilter(){
												@Override
												public boolean filter(Message msg) {
													return msg!=null && msg.containsKey("MessageType")
															&& msg.get("MessageType") == MessageType.FIND_SUCCESSOR_RESPONSE;
												}
											}, 5000, 
											new OnMessageReceivedListener(){
												@Override
												public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
													result.put("result", msg.get("result"));
													result.put("success", true);
												}
												@Override
												public void OnReceiveError(CommunicationManager manager, int id) {
													result.put("success", false);
													System.err.println("Unable to receive FIND_SUCCESSOR_RESPONSE!");
												}		
											}, true);
		} catch (IOException e) {
			System.err.println("Unable to send FIND_SUCCESSOR to "+predecessor);
			throw new OperationFailsException("Unable to send FIND_SUCCESSOR to "+predecessor);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Long)result.get("result");
	}
	
	private long find_predecessor(long target) throws OperationFailsException{
		return help_other_find_predecessor(new Message().put("MessageType", MessageType.FIND_PREDECESSOR).
														 put("target", target));
	}
	
	@SuppressWarnings("unchecked")
	private long help_other_find_predecessor(Message msg) throws OperationFailsException{
		long target = (Long) msg.get("target");
		if(IDRing.isBetween(target, id, successor) || successor == target) return id;
		long queryNode = fingerTable.closest_preceding_finger(target);
		if(msg.containsKey("origin")){
			if((Long)msg.get("origin") == id){
				System.err.println("Reach a circle!");				
				throw new OperationFailsException();
			}
		}else
			msg.put("origin", id);
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			manager.sendMessageForResponse(id2link.get(queryNode), msg,
										   new MessageFilter(){
											@Override
											public boolean filter(Message msg) {
												return msg!=null && msg.containsKey("MessageType")
														&& msg.get("MessageType") == MessageType.FIND_PREDECESSOR_RESPONSE;
												}
											}, 5000, 
											new OnMessageReceivedListener(){
												@Override
												public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
													if((Boolean)msg.get("success")){
														result.put("result", msg.get("result"));
														result.put("success", true);
													}else
														result.put("success", false);
												}

												@Override
												public void OnReceiveError(CommunicationManager manager, int id) {
													result.put("success", false);
													System.err.println("Unable to receive FIND_PREDECESSOR_RESPONSE!");
												}
												
											}, true);
		} catch (IOException e) {
			System.err.println("Unable to send FIND_PREDECESSOR to "+queryNode);
			throw new OperationFailsException("Unable to send FIND_PREDECESSOR to "+queryNode);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Long) result.get("result");
	}
	
	private class MessageListener implements OnMessageReceivedListener{

		@Override
		public void OnMessageReceived(final CommunicationManager manager, final int id, final Message msg) {
			if(!msg.containsKey("MessageType")) return;
			try {
				switch((MessageType)msg.get("MessageType")){
					case FIND_PREDECESSOR:
						new Thread(){
							@Override
							public void run(){
								Message reply = new Message().put("MessageType", MessageType.FIND_PREDECESSOR_RESPONSE);
								try {
									long result = help_other_find_predecessor(msg);
									reply.put("success", true);
									reply.put("result", result);
								} catch (OperationFailsException e) {
									reply.put("success", false);
								}
								try {
									manager.sendMessage(id, reply);
								} catch (IOException e) {
									System.err.println("Error: Unable to send FIND_PREDECESSOR_RESPONSE!");
								}
							}
						}.start();
						break;
					case FIND_SUCCESSOR:
						manager.sendMessage(id, new Message().put("MessageType", MessageType.FIND_SUCCESSOR_RESPONSE).
															  put("result", successor));
						break;
					default:
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Error occurs when processing message "+msg+"!");
			}
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("Error occurs when receiving message!");
			manager.closeConnection(id);
		}
		
	}
	
	private class ConnectionListener implements OnConnectionListener{
		@Override
		public void OnConnected(CommunicationManager manager, int id) {
			manager.setOnMessageReceivedListener(id, new OnMessageReceivedListener(){
				@Override
				public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
					if(!msg.containsKey("MessageType")) return;
					if(!(msg.get("MessageType") == MessageType.CONNECTION_REQUEST)) return;
					long nodeId = (Long) msg.get("id");
					if(id2link.containsKey(nodeId)){
						System.err.println("Duplicate connection from "+nodeId+", rejected!");
						manager.closeConnection(id);
						return;
					}
					manager.setOnMessageReceivedListener(id, new MessageListener());
					try {
						manager.sendMessage(id, new Message().put("MessageType", MessageType.CONNECTION_ACCEPTED));
					} catch (IOException e) {
						manager.closeConnection(id);
						return;
					}
					link2id.put(id, nodeId);
					id2link.put(nodeId, id);
					System.out.println("Connected to chord node "+nodeId);
				}
				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					manager.closeConnection(id);
				}			
			});		
		}

		@Override
		public void OnConnectFail(CommunicationManager manager) {
			System.err.println("Chord: error occurs when connecting!");
		}
		
	}
	
	/**
	 * Test
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException{
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		ChordNode n0 = new ChordNode(cluster, 0);
		ChordNode n1 = new ChordNode(cluster, 1);
		n0.successor = n1.id;
		n1.successor = n0.id;
		long index = (n1.id - n0.id + (1L<<32)) % (1L<<32);
		int k = (int)(Math.log(index)/Math.log(2));
		System.out.println(k);
		for(int i=0; i<=k; i++){
			n0.fingerTable.setSuccessor(i, n1.id);
		}
		System.out.println(n0.fingerTable);
		index = (n0.id - n1.id + (1L<<32)) % (1L<<32);
		k = (int)(Math.log(index)/Math.log(2));
		System.out.println(k);
		for(int i=0; i<=k; i++){
			n1.fingerTable.setSuccessor(i, n0.id);
		}
		System.out.println(n1.fingerTable);
		try {
			for(int i=0; i<1000; i++){
				long target = (long)(Math.random()*(1L<<32-1));
				long should = IDRing.isBetween(target, n0.id, n1.id+1)? n0.id: n1.id;
				long r0 = n0.find_predecessor(target);
				assert(r0 == should): "Target is "+target+". Got "+r0+", which should be "+should;
				long r1 = n1.find_predecessor(target);
				assert(r1 == should): "Target is "+target+". Got "+r1+", which should be "+should;
				//test find_successor!
				should = IDRing.isBetween(target, n0.id-1, n1.id)? n1.id: n0.id;
				r0 = n0.find_successor(target);
				assert(r0 == should): "Target is "+target+". Got "+r0+", which should be "+should;
				r1 = n1.find_successor(target);
				assert(r1 == should): "Target is "+target+". Got "+r1+", which should be "+should;
			}
			System.out.println("Pass!");
		} catch (OperationFailsException e) {
			e.printStackTrace();
		}
		
	}

}
