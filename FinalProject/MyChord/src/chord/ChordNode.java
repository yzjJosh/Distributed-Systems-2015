package chord;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import backups.FusedRepository;
import backups.WriterReaderLock;
import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;
import dataStructures.Coder;
import dataStructures.FusionBackupHashMap;
import exceptions.DecodeException;
import exceptions.OperationFailsException;

public class ChordNode {
	
	private final CommunicationManager manager;
	private final long id;
	private final ConcurrentHashMap<Integer, Long> link2id;
	private final ConcurrentHashMap<Long, Integer> id2link;
	private final HashMap<Long, Integer> id2primary;
	private final FingerTable fingerTable;
	private long predecessor;
	private final Object predecessorLock = new Object();
	private long successor;
	private final Object successorLock = new Object();
	private long successorOfSuccessor;
	private final Object successorOfSuccessorLock = new Object();
	private final Map<Serializable, Serializable> data;
	private final WriterReaderLock dataLock = new WriterReaderLock(10);
	private final Map<Integer, String> backupNodesInfo;
	private final Coder<Serializable> coder;
	private final StabilizeThread stabilizeThread;
	
	
	public ChordNode(Map<Integer, String> chordNodesInfo, int index, Map<Integer, String> backupNodesInfo, Coder<Serializable> coder){
		if(chordNodesInfo == null || !chordNodesInfo.containsKey(index) || chordNodesInfo.get(index) == null)
			throw new IllegalArgumentException();
		manager = new CommunicationManager();
		link2id = new ConcurrentHashMap<Integer, Long>();
		id2link = new ConcurrentHashMap<Long, Integer>();
		id2primary = new HashMap<Long, Integer>();
		this.backupNodesInfo = backupNodesInfo;
		id = hash(chordNodesInfo.get(index));
		predecessor = id;
		successor = id;
		successorOfSuccessor = id;
		fingerTable = new FingerTable(id);
		data = new FusionBackupHashMap<Serializable, Serializable>(backupNodesInfo, index, coder);
		this.coder = coder;
		LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
		for(final Map.Entry<Integer, String> entry: chordNodesInfo.entrySet()){
			id2primary.put(hash(entry.getValue()), entry.getKey());
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
		try {
			join();
			transferData();
		} catch (OperationFailsException e) {
			e.printStackTrace();
		}
		stabilizeThread = new StabilizeThread();
		stabilizeThread.start();
		int port = Integer.parseInt(chordNodesInfo.get(index).split(":")[1]);
		manager.waitForConnection(port, new ConnectionListener());
		System.out.println("ChordNode starts, id="+id);
		System.out.println("Waiting for connection at port "+port);
	}
	
	private long hash(Object obj){
		return obj.hashCode() & 0x00000000ffffffffL;
	}
	
	public Serializable put(Serializable key, Serializable value) throws OperationFailsException{
		long keyId = hash(key);
		long successor = find_successor(keyId);
		if(successor == id){
			dataLock.writerLock();
			Serializable ret = data.put(key, value);
			dataLock.writerUnlock();
			return ret;
		}
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try{
			manager.sendMessageForResponse(id2link.get(successor), new Message().put("MessageType", MessageType.PUT).
					put("key", key).
					put("value", value), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.PUT_RESPONSE
									&& msg.get("keyId").equals(keyId);
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
		long keyId = hash(key);
		long successor = find_successor(keyId);
		if(successor == id){
			dataLock.readerLock();
			Serializable ret = data.get(key);
			dataLock.readerUnlock();
			return ret;
		}
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try{
			manager.sendMessageForResponse(id2link.get(successor), new Message().put("MessageType", MessageType.GET).
					put("key", key), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.GET_RESPONSE
									&& msg.get("keyId").equals(keyId);
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
	
	private long get_successor(long node) throws OperationFailsException{
		if(node == id) return successor;
		if(!id2link.containsKey(node))
			throw new OperationFailsException(node+" is not the id of a connected chord node!");
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try {
			manager.sendMessageForResponse(id2link.get(node), new Message().put("MessageType", MessageType.GET_SUCCESSOR), 
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
			System.err.println("Unable to send GET_SUCCESSOR to "+node);
			throw new OperationFailsException("Unable to send GET_SUCCESSOR to "+node);
		}
		if(!(Boolean)result.get("success"))
			throw new OperationFailsException();
		return (Long)result.get("result");
	}
	
	private long find_successor(long target) throws OperationFailsException{
		return get_successor(find_predecessor(target));
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
									&& msg.get("MessageType") == MessageType.FIND_PREDECESSOR_RESPONSE
									&& msg.get("target").equals(target);
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
		} catch (Exception e) {
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
		boolean success = false;
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
										&& msg.get("MessageType") == MessageType.FIND_SUCCESSOR_RESPONSE
										&& msg.get("target").equals(ChordNode.this.id);
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
			if((Boolean)result.get("success")){
				success = true;
				break;
			}
		}
		if(!success)
			throw new OperationFailsException("Unable to join!");
		long nextSuccessor = get_successor(successor);
		synchronized(successorOfSuccessorLock){
			successorOfSuccessor = nextSuccessor;
		}
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
					}, 10000, 
					new OnMessageReceivedListener(){
						@SuppressWarnings("unchecked")
						@Override
						public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {			
							HashMap<Serializable, Serializable> response = (HashMap<Serializable, Serializable>) msg.get("result");
							System.out.println("Got "+response.size()+" entries from successor!");
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
	
	private void recoverDataOfNode(long nodeId) throws OperationFailsException{
		if(nodeId == id) return;
		if(!id2primary.containsKey(nodeId))
			throw new OperationFailsException(nodeId+" is not a valid node id!");
		int primary = id2primary.get(nodeId);
		System.out.println("Recovering data for "+nodeId+"...");
		FusionBackupHashMap<Serializable, Serializable> recover = new FusionBackupHashMap<Serializable, Serializable>(backupNodesInfo, primary, coder);
		System.out.println("Successfully recovered "+recover.size()+" entries!");
		dataLock.writerLock();
		for(Map.Entry<Serializable, Serializable> entry: recover.entrySet())
			data.put(entry.getKey(), entry.getValue());
		dataLock.writerUnlock();
		System.out.println("Updating repository...");
		recover.clear();
		System.out.println("Repository updated!");
		recover.disConnect();
		System.out.println("Recover ends!");
	}
	
	private void ask_other_to_recover_data(long recoveredNode, long operationNode) throws OperationFailsException{
		if(operationNode == id){
			recoverDataOfNode(recoveredNode);
			return;
		}
		final HashMap<String, Object> result = new HashMap<String, Object>();
		try{
			manager.sendMessageForResponse(id2link.get(operationNode), new Message().
					put("MessageType", MessageType.RECOVER_DATA).
					put("target", recoveredNode), 
					new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg != null && msg.containsKey("MessageType")
									&& msg.get("MessageType") == MessageType.RECOVER_DATA_RESPONSE;
						}
					}, 10000, 
					new OnMessageReceivedListener(){
						@Override
						public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
							result.put("success", msg.get("success"));
						}
						@Override
						public void OnReceiveError(CommunicationManager manager, int id) {
							result.put("success", false);
						}
					}, true);
		}catch (IOException e){
			System.err.println("Error: Unable to send RECOVER_DATA to "+operationNode);
			throw new OperationFailsException("Error: Unable to send RECOVER_DATA to "+operationNode);
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
			} catch (Exception e) {
				System.err.println("Error: Unable to send GET_PREDECESSOR!");
				System.out.println("Node "+id+": Successor "+successor+" crashes! Change successor into "+successorOfSuccessor+"!");
				ask_other_to_recover_data(successor, successorOfSuccessor);
				synchronized(successorLock){
					successor = successorOfSuccessor;
					fingerTable.setSuccessor(0, successor);
				}
				result.put("success", true);
				result.put("result", id);
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
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Node "+id+": Unable to send NOTIFY to "+successor+"!");
				throw new OperationFailsException("Unable to send NOTIFY!");
			}
		long nextSuccessor = get_successor(successor);
		synchronized(successorOfSuccessorLock){
			successorOfSuccessor = nextSuccessor;
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
									reply.put("target", msg.get("target"));
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
									reply.put("target", msg.get("target"));
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
						long nodeId = (Long) msg.get("id");
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
						Message reply = new Message().put("MessageType", MessageType.PUT_RESPONSE).put("keyId", hash(key)).put("result", result);
						manager.sendMessage(id, reply);
						break;
					case GET:
						key = msg.get("key");
						dataLock.readerLock();
						result = data.get(key);
						dataLock.readerUnlock();
						reply = new Message().put("MessageType", MessageType.GET_RESPONSE).put("keyId", hash(key)).put("result", result);
						manager.sendMessage(id, reply); 
						break;
					case RECOVER_DATA:
						reply = new Message().put("MessageType", MessageType.RECOVER_DATA_RESPONSE);
						try{
							recoverDataOfNode((long) msg.get("target"));
							reply.put("success", true);
						}catch(OperationFailsException e){
							reply.put("success", false);
						}
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
			long nodeId = link2id.remove(id);
			id2link.remove(nodeId);
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
		
		private boolean exit = false;
		
		public void exit(){
			exit = true;
			this.interrupt();
		}
		
		@Override
		public void run(){
			while(!exit){
				try {
					stabilize();
					if(exit) return;
					fix_fingers();
				} catch (OperationFailsException e) {
					e.printStackTrace();
				}
				if(exit) return;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}
		}
	}
	
	//-----------------------------------------------------------------------------------------------
	//Following code is for testing purpose only
	
	private static int next(int index, int len){
		if(index == len-1) return 0;
		return index+1;
	}
	
	private static int prev(int index, int len){
		if(index == 0) return len-1;
		return index-1;
	}
	
	private static class IntegerCoder implements Coder<Serializable>{

		private int toInt(double d){
			return (int)Math.round(d);
		}

		@Override
		public ArrayList<Double> encode(Serializable target) {
			ArrayList<Double> ret = new ArrayList<Double>();
			if(target == null){
				ret.add(1.0);
				ret.add(1.0);
			}else
				ret.add((double)(((Integer)target).intValue()));
			return ret;
		}


		@Override
		public Serializable decode(ArrayList<Double> source) {
			if(source == null || source.size() == 0)
				throw new DecodeException("Unable to decode from empty list!");
			if(source.size()>1 && toInt(source.get(0)) == 1 && toInt(source.get(1)) == 1)
				return null;
			return toInt(source.get(0));
		}
	}
	
	/**
	 * Test
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws Exception{
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":22446");
		cluster.put(2, ip+":32547");
		cluster.put(3, ip+":42648");
		cluster.put(4, ip+":52749");
		cluster.put(5, ip+":40000");
		HashMap<Integer, String> backupNodes = new HashMap<Integer, String>();
		backupNodes.put(0, ip+":13579");
		backupNodes.put(1, ip+":13580");
		backupNodes.put(2, ip+":13581");
		FusedRepository[] repositories = new FusedRepository[3];
		for(int i=0; i<repositories.length; i++)
			repositories[i] = new FusedRepository(2, cluster.size(), i, backupNodes);
		ChordNode[] nodes = new ChordNode[5];
		for(int i=0; i<nodes.length; i++)
			nodes[i] = new ChordNode(cluster, i, backupNodes, new IntegerCoder());
		Thread.sleep(10000);
		for(int i=0; i<nodes.length; i++){
			assert(nodes[i].successor == nodes[next(i, nodes.length)].id): "Got "+nodes[i].successor+", which should be "+nodes[next(i, nodes.length)].id;
			assert(nodes[i].successorOfSuccessor == nodes[next(next(i, nodes.length), nodes.length)].id): 
				"Got "+nodes[i].successorOfSuccessor+", which should be "+nodes[next(next(i, nodes.length), nodes.length)].id;
		}
		for(int i=0; i<nodes.length; i++)
			assert(nodes[i].predecessor == nodes[prev(i, nodes.length)].id): "Got "+nodes[i].predecessor+", which should be "+nodes[prev(i, nodes.length)].id;
		System.out.println("Stabilize successful!");
		HashMap<Long, Integer> data = new HashMap<Long, Integer>();
		for(int i=0; i<1000; i++){
			int index = (int)(Math.random()*(nodes.length));
			long key = (long)(Math.random()*(1L<<32));
			int value = (int)(Math.random()*Integer.MAX_VALUE);
			nodes[index].put(key, value);
			for(int j=0; j<nodes.length; j++){
				Serializable result = nodes[j].get(key);
				assert(result != null && result.equals(value)):"Key is "+key+", got "+result+", which should be "+value;
			}
			int newValue = (int)(Math.random()*Integer.MAX_VALUE);
			index = (int)(Math.random()*(nodes.length));
			assert(nodes[index].put(key, newValue).equals(value));
			data.put(key, newValue);
		}
		for(int i=0; i<100; i++){
			int index = (int)(Math.random()*(nodes.length));
			long key = nodes[2].id + (long)(Math.random()*(nodes[3].id - nodes[2].id));
			int value = (int)(Math.random()*Integer.MAX_VALUE);
			nodes[index].put(key, value);
			data.put(key, value);
		}
		ChordNode newNode = new ChordNode(cluster, 5, backupNodes, new IntegerCoder());
		StabilizeThread.sleep(5000);
		assert(newNode.successor == nodes[3].id);
		assert(newNode.successorOfSuccessor == nodes[4].id);
		assert(newNode.predecessor == nodes[2].id);
		assert(nodes[3].predecessor == newNode.id);
		assert(nodes[2].successor == newNode.id);
		assert(nodes[1].successorOfSuccessor == newNode.id);
		System.out.println("New node has been stabilized!");
		for(Long key: data.keySet()){
			Integer result = (Integer) newNode.get(key);
			assert(result.equals(data.get(key))): "Got "+result+", which should be "+data.get(key);
		}
		for(int i=0; i<1000; i++){
			long key = (long)(Math.random()*(1L<<32));
			int value = (int)(Math.random()*Integer.MAX_VALUE);
			newNode.put(key, value);
			for(int j=0; j<nodes.length; j++){
				Serializable result = nodes[j].get(key);
				assert(result != null && result.equals(value)):"Key is "+key+", got "+result+", which should be "+value;
			}
			int newValue = (int)(Math.random()*Integer.MAX_VALUE);
			int index = (int)(Math.random()*(nodes.length));
			assert(nodes[index].put(key, newValue).equals(value));
			data.put(key, newValue);
		}
		for(Long key: data.keySet()){
			for(int j=0; j<nodes.length; j++){
				Serializable result = nodes[j].get(key);
				assert(result != null && result.equals(data.get(key))):"Key is "+key+", got "+result+", which should be "+data.get(key);
			}
			assert(newNode.get(key).equals(data.get(key)));
		}
		System.out.println("Simulation node crash on node "+newNode.id);
		((FusionBackupHashMap<Serializable, Serializable>)newNode.data).disConnect();
		newNode.manager.close();
		newNode.stabilizeThread.exit();
		System.out.println("Waiting for recovery...");
		Thread.sleep(10000);
		System.out.println("Testing recovery result");
		for(Long key: data.keySet()){
			for(int j=0; j<nodes.length; j++){
				Serializable result = nodes[j].get(key);
				assert(result != null && result.equals(data.get(key))):"Key is "+key+", got "+result+", which should be "+data.get(key);
			}
			assert(newNode.get(key).equals(data.get(key)));
		}
		System.out.println("Pass!");
		
	}

}
