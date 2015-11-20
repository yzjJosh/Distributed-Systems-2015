package chord;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import backups.WriterReaderLock;
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
	private final Object predecessorLock = new Object();
	private long successor;
	private final Object successorLock = new Object();
	private final Map<Serializable, Serializable> data;
	private final WriterReaderLock dataLock = new WriterReaderLock(10);
	
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
		data = new HashMap<Serializable, Serializable>();
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
		try {
			join();
			transferData();
		} catch (OperationFailsException e) {
			e.printStackTrace();
		}
		new StabilizeThread().start();
		System.out.println("ChordNode starts, id="+id);
		System.out.println("Waiting for connection at port "+port);
	}
	
	private long hash(Object obj){
		return obj.hashCode() & 0x00000000ffffffffL;
	}
	
	public Serializable put(Serializable key, Serializable value) throws OperationFailsException{
		long successor = find_successor(hash(key));
		if(successor == id) 
			return data.put(key, value);
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try{
			manager.sendMessageForResponse(id2link.get(successor), new Message().put("MessageType", MessageType.PUT).
					put("key", key).
					put("value", value), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.PUT_RESPONSE;
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
						}						
					}, true);
		} catch(IOException e){
			System.err.println("Error: Unable to send PUT!");
			result.put("success", false);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Serializable)result.get("result");
	}
	
	public Serializable get(Serializable key) throws OperationFailsException{
		long successor = find_successor(hash(key));
		if(successor == id)
			return data.get(key);
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try{
			manager.sendMessageForResponse(id2link.get(successor), new Message().put("MessageType", MessageType.GET).
					put("key", key), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.GET_RESPONSE;
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
						}						
					}, true);
		} catch(IOException e){
			System.err.println("Error: Unable to send PUT!");
			result.put("success", false);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Serializable)result.get("result");
	}
	
	private long find_successor(long target) throws OperationFailsException{
		long predecessor = find_predecessor(target);
		if(predecessor == id) return successor;
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			manager.sendMessageForResponse(id2link.get(predecessor), new Message().put("MessageType", MessageType.GET_SUCCESSOR), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg!=null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.GET_SUCCESSOR_RESPONSE;
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
							System.err.println("Unable to receive GET_SUCCESSOR_RESPONSE!");
						}		
					}, true);
		} catch (IOException e) {
			System.err.println("Unable to send GET_SUCCESSOR to "+predecessor);
			throw new OperationFailsException("Unable to send GET_SUCCESSOR to "+predecessor);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Long)result.get("result");
	}
	
	private long find_predecessor(long target) throws OperationFailsException{
		return help_other_find_predecessor(new Message().put("MessageType", MessageType.FIND_PREDECESSOR).
														 put("target", target));
	}
	
	private long help_other_find_predecessor(Message msg) throws OperationFailsException{
		long target = (Long) msg.get("target");
		if(IDRing.isBetween(target, id, successor) || successor == target) return id;
		long queryNode = fingerTable.closest_preceding_finger(target);
		if(!msg.containsKey("origin"))
			msg.put("origin", id);
		if((Long)msg.get("origin") == queryNode){
			System.err.println("Reach a circle!");				
			throw new OperationFailsException();
		}
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
	
	private void join() throws OperationFailsException{
		synchronized(predecessorLock){
			predecessor = -1;
		}
		if(link2id.size() == 0) return;
		for(int link: link2id.keySet()){
			final HashMap<String, Object> result = new HashMap<String, Object>();
			try {
				manager.sendMessageForResponse(link, 
						new Message().put("MessageType", MessageType.FIND_SUCCESSOR).
									  put("target", ChordNode.this.id),
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
								if((Boolean)msg.get("success")){
									synchronized(successorLock){
										successor = (Long)msg.get("result");
										fingerTable.setSuccessor(0, successor);
									}
									result.put("success", true);
								}else
									result.put("success", false);
							}
							@Override
							public void OnReceiveError(CommunicationManager manager, int id) {
								result.put("success", false);
							}					
						}, true);
			} catch (IOException e) {
				result.put("success", false);
			}
			if((Boolean)result.get("success"))
				return;
		}
		throw new OperationFailsException("Unable to join!");
	}
	
	private void transferData() throws OperationFailsException{
		if(successor == id) return;
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			manager.sendMessageForResponse(id2link.get(successor), new Message().put("MessageType", MessageType.TRANSFER_DATA_REQUEST).
					put("id", id), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType") && 
									msg.get("MessageType") == MessageType.TRANSFER_DATA_RESPONSE;
						}
					}, 5000, 
					new OnMessageReceivedListener(){
						@SuppressWarnings("unchecked")
						@Override
						public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {			
							HashMap<Serializable, Serializable> response = (HashMap<Serializable, Serializable>) msg.get("result");
							dataLock.writerLock();
							for(Map.Entry<Serializable, Serializable> entry: response.entrySet())
								data.put(entry.getKey(), entry.getValue());
							dataLock.writerUnlock();
							try {
								manager.sendMessage(id, new Message().put("MessageType", MessageType.TRANSFER_DATA_ACK));
							} catch (IOException e) {
								result.put("success", false);
								System.err.println("Unable to send back TRANSFER_DATA_ACK!");
								return;
							}
							result.put("success", true);
						}
						@Override
						public void OnReceiveError(CommunicationManager manager, int id) {
							result.put("success", false);
							System.err.println("Unable to receive TRANSFER_DATA_RESPONSE!!");
						}												
					}, true);
		} catch (IOException e) {
			result.put("success", false);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
	}
	
	private void stabilize() throws OperationFailsException{
		final HashMap<String, Object> result = new HashMap<String, Object>();
		long x = predecessor;
		if(successor != id){
			try {
				manager.sendMessageForResponse(id2link.get(successor), 
						new Message().put("MessageType", MessageType.GET_PREDECESSOR), 
						new MessageFilter(){
							@Override
							public boolean filter(Message msg) {
								return msg != null && msg.containsKey("MessageType")
										&& msg.get("MessageType") == MessageType.GET_PREDECESSOR_RESPONSE;
							}
						}, 5000, 
						new OnMessageReceivedListener(){
							@Override
							public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
								result.put("success", true);
								result.put("result", msg.get("result"));
							}
							@Override
							public void OnReceiveError(CommunicationManager manager, int id) {
								System.err.println("Error: Unable to receive GET_PREDECESSOR_RESPONSE!");
								result.put("success", false);
							}
							
						}, true);
			} catch (IOException e) {
				System.err.println("Error: Unable to send GET_PREDECESSOR!");
				throw new OperationFailsException("Unable to send GET_PREDECESSOR");
			}
			if(!(Boolean)result.get("success"))
				throw new OperationFailsException("Error: Unable to receive GET_PREDECESSOR_RESPONSE!");
			x = (Long) result.get("result");
		}
		if(IDRing.isBetween(x, id, successor))
			synchronized(successorLock){
				successor = x;
				fingerTable.setSuccessor(0, successor);
			}
		if(successor != id)
			try {
				manager.sendMessage(id2link.get(successor), new Message().
						put("MessageType", MessageType.NOTIFY).
						put("target", id));
			} catch (IOException e) {
				System.err.println("Unable to send NOTIFY!");
				throw new OperationFailsException("Unable to send NOTIFY!");
			}
	}
	
	private void notifyFromPredecessor(long target){
		if(predecessor == -1 || IDRing.isBetween(target, predecessor, id))
			synchronized(predecessorLock){
				predecessor = target;
			}
	}
	
	private void fix_fingers() throws OperationFailsException{
		int i = (int) (Math.random()*32);
		fingerTable.setSuccessor(i, find_successor(fingerTable.getStart(i)));
	}
	
	private class MessageListener implements OnMessageReceivedListener{

		@Override
		public void OnMessageReceived(final CommunicationManager manager, final int id, final Message msg) {
			if(!msg.containsKey("MessageType")) return;
			try {
				switch((MessageType)msg.get("MessageType")){
					case GET_PREDECESSOR:
						manager.sendMessage(id, new Message().put("MessageType", MessageType.GET_PREDECESSOR_RESPONSE).
								  							  put("result", predecessor));
						break;
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
					case GET_SUCCESSOR:
						manager.sendMessage(id, new Message().put("MessageType", MessageType.GET_SUCCESSOR_RESPONSE).
															  put("result", successor));
						break;
					case FIND_SUCCESSOR:
						new Thread(){
							@Override
							public void run(){
								Message reply = new Message().put("MessageType", MessageType.FIND_SUCCESSOR_RESPONSE);
								try{
									long result = find_successor((Long)msg.get("target"));
									reply.put("result", result);
									reply.put("success", true);
								}catch(OperationFailsException e){
									reply.put("success", false);
								}
								try {
									manager.sendMessage(id, reply);
								} catch (IOException e) {
									System.err.println("Error: Unable to send JOIN_RESPONSE!");
								}
							}
						}.start();
						break;
					case NOTIFY:
						notifyFromPredecessor((Long)msg.get("target"));
						break;
					case TRANSFER_DATA_REQUEST:
						final HashMap<Serializable, Serializable> transfer = new HashMap<Serializable, Serializable>();
						long nodeId = (long) msg.get("id");
						dataLock.readerLock();
						for(Map.Entry<Serializable, Serializable> entry: data.entrySet())
							if(hash(entry.getKey()) <= nodeId)
								transfer.put(entry.getKey(), entry.getValue());
						dataLock.readerUnlock();
						manager.sendMessageForResponse(id, new Message().put("MessageType", MessageType.TRANSFER_DATA_RESPONSE).
								put("result", transfer),
								new MessageFilter(){
									@Override
									public boolean filter(Message msg) {
										return msg != null && msg.containsKey("MessageType")
												&& msg.get("MessageType") == MessageType.TRANSFER_DATA_ACK;
									}
								}, 5000, 
								new OnMessageReceivedListener(){
									@Override
									public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
										dataLock.writerLock();
										for(Map.Entry<Serializable, Serializable> entry: transfer.entrySet())
											data.remove(entry.getKey());
										dataLock.writerUnlock();
									}
									@Override
									public void OnReceiveError(CommunicationManager manager, int id) {
										System.err.println("Unable to receive TRANSFER_DATA_ACK!");
									}
								}, false);
						break;
					case PUT:
						Serializable key = msg.get("key");
						Serializable value = msg.get("value");
						dataLock.writerLock();
						Serializable result = data.put(key, value);
						dataLock.writerUnlock();
						Message reply = new Message().put("MessageType", MessageType.PUT_RESPONSE).put("result", result);
						manager.sendMessage(id, reply);
						break;
					case GET:
						key = msg.get("key");
						dataLock.readerLock();
						result = data.get(key);
						dataLock.readerUnlock();
						reply = new Message().put("MessageType", MessageType.GET_RESPONSE).put("result", result);
						manager.sendMessage(id, reply); 
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
	
	private class StabilizeThread extends Thread{
		
		@Override
		public void run(){
			while(true){
				try {
					stabilize();
					fix_fingers();
				} catch (OperationFailsException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(1500);
				} catch (InterruptedException e) {}
			}
		}
	}
	
	private static int next(int index, int len){
		if(index == len-1) return 0;
		return index+1;
	}
	
	private static int prev(int index, int len){
		if(index == 0) return len-1;
		return index-1;
	}
	
	/**
	 * Test
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws Exception{
		/*HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		cluster.put(5, ip+":15000");
		ChordNode[] nodes = new ChordNode[5];
		for(int i=0; i<nodes.length; i++)
			nodes[i] = new ChordNode(cluster, i);
		Thread.sleep(5000);
		for(int i=0; i<nodes.length; i++)
			assert(nodes[i].successor == nodes[next(i, nodes.length)].id): "Got "+nodes[i].successor+", which should be "+nodes[next(i, nodes.length)].id;
		for(int i=0; i<nodes.length; i++)
			assert(nodes[i].predecessor == nodes[prev(i, nodes.length)].id): "Got "+nodes[i].predecessor+", which should be "+nodes[prev(i, nodes.length)].id;
		for(int i=0; i<100; i++){
			int index = (int)(Math.random()*(nodes.length));
			long key = (long)(Math.random()*(1L<<32));
			int value = (int)(Math.random()*Integer.MAX_VALUE);
			nodes[index].put(key, value);
			for(int j=0; j<nodes.length; j++)
				assert((Integer)nodes[j].get(key) == value);
			long newValue = (int)(Math.random()*Integer.MAX_VALUE);
			index = (int)(Math.random()*(nodes.length));
			assert((Integer)nodes[index].put(key, newValue) == value);
		}
		//nodes[0].put(key, value)
		
		System.out.println("Pass!");*/
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		ChordNode n0 = new ChordNode(cluster, 0);
		ChordNode n1 = new ChordNode(cluster, 1);
		ChordNode n2 = new ChordNode(cluster, 2);
		ChordNode n3 = new ChordNode(cluster, 3);
		ChordNode n4 = new ChordNode(cluster, 4);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println(n0.fingerTable);
		System.out.println(n1.fingerTable);
		System.out.println(n2.fingerTable);
		System.out.println(n3.fingerTable);
		System.out.println(n4.fingerTable);
		assert(n0.successor == n1.id): "Got "+n0.successor+", which should be "+n1.id;
		assert(n1.successor == n2.id): "Got "+n1.successor+", which should be "+n2.id;
		assert(n2.successor == n3.id): "Got "+n2.successor+", which should be "+n3.id;
		assert(n3.successor == n4.id): "Got "+n3.successor+", which should be "+n4.id;
		assert(n4.successor == n0.id): "Got "+n4.successor+", which should be "+n0.id;
		assert(n1.predecessor == n0.id): "Got "+n1.predecessor+", which should be "+n0.id;
		assert(n2.predecessor == n1.id): "Got "+n2.predecessor+", which should be "+n1.id;
		assert(n3.predecessor == n2.id): "Got "+n3.predecessor+", which should be "+n2.id;
		assert(n4.predecessor == n3.id): "Got "+n4.predecessor+", which should be "+n3.id;
		assert(n0.predecessor == n4.id): "Got "+n0.predecessor+", which should be "+n4.id;
		System.out.println("Pass!");
		
	}

}
