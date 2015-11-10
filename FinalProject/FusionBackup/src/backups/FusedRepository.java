package backups;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Semaphore;

import org.jblas.DoubleMatrix;
import org.jblas.Solve;
import org.jblas.ranges.IndicesRange;
import org.jblas.ranges.IntervalRange;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;
import constants.MessageType;
import constants.NodeType;
import constants.UpdateType;
import exceptions.BackupFailureException;
import exceptions.RecoverFailureException;

/**
 * Repository of fused backup data.
 * @author Josh
 *
 */
public class FusedRepository {
	
	public final int id;
	public final int nodeSize;
	public final int volume;
	private final DoubleMatrix fuseVector;
	private final AuxiliaryDataStructure<Serializable>[] auxDataStructures;
	private final ArrayList<FusedNode> dataStack;
	private final CommunicationManager manager;
	private final HashMap<Integer, Integer> repoid2connection;
	private final HashMap<Integer, Integer> connection2repoid;
	private final HashMap<Integer, Integer> clientid2connection;
	private final HashMap<Integer, Integer> connection2clientid;
	private final WriterReaderLock repomapLock = new WriterReaderLock(20);
	private final WriterReaderLock clientmapLock = new WriterReaderLock(20);
	private final FusedNode lockNode;
	
	/**
	 * Initialize this repository.
	 * @param nodeSize The size of each node in this repository
	 * @param volume The maximum number of primaries that can be fused into this repository
	 * @param id the id of this repository
	 * @param cluster a map which contains id-ip:port pair for nodes in this cluster, for example (2, "192.168.1.1:12345")
	 */
	@SuppressWarnings("unchecked")
	public FusedRepository(int nodeSize, int volume, int id, HashMap<Integer, String> cluster){
		if(nodeSize <= 0)
			throw new IllegalArgumentException("Illegal nodeSize "+nodeSize);
		if(volume <= 0)
			throw new IllegalArgumentException("Illegal volume "+volume);
		this.id = id;
		this.nodeSize = nodeSize;
		this.volume = volume;
		this.fuseVector = generateFuseVector();
		this.auxDataStructures = (AuxiliaryDataStructure<Serializable>[])new AuxiliaryDataStructure[volume];
		this.dataStack = new ArrayList<FusedNode>();
		this.manager = new CommunicationManager();
		this.repoid2connection = new HashMap<Integer, Integer>();
		this.connection2repoid = new HashMap<Integer, Integer>();
		this.clientid2connection = new HashMap<Integer, Integer>();
		this.connection2clientid = new HashMap<Integer, Integer>();
		this.lockNode = new FusedNode(nodeSize, fuseVector, -1);
		LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
		for(Map.Entry<Integer, String> entry : cluster.entrySet()){
			final int nodeId = entry.getKey();
			if(nodeId == id) continue;
			String[] temp = entry.getValue().split(":");
			final String ip = temp[0];
			final int port = Integer.parseInt(temp[1]);
			final Semaphore semaphore = new Semaphore(0);
			semaphores.add(semaphore);
			new Thread(){
				@Override
				public void run(){
					int connection = -1;
					try {
						connection = manager.connect(ip, port);
						manager.sendMessageForResponse(connection, 
								new Message().put("MessageType", MessageType.CONNECT_REQUEST).
										put("NodeType", NodeType.REPOSITORY).
										put("id", FusedRepository.this.id),
								new MessageFilter(){
									@Override
									public boolean filter(Message msg) {
										return msg != null && msg.containsKey("MessageType")
											   && msg.get("MessageType") == MessageType.CONNECT_ACCEPTED;
									}
								}, 5000, 
								new OnMessageReceivedListener(){
									@Override
									public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
											repomapLock.writerLock();
											repoid2connection.put(nodeId, id);
											connection2repoid.put(id, nodeId);
											repomapLock.writerUnlock();
											manager.setOnMessageReceivedListener(id, new ClusterMessageListener());
											System.out.println("Connected to repository node "+nodeId);
									}
									@Override
									public void OnReceiveError(CommunicationManager manager, int id) {
										manager.closeConnection(id);
									}	
								}, true);
								
					} catch (IOException e) {
						if(connection >= 0)
							manager.closeConnection(connection);
					}
					semaphore.release();
				}
			}.start();
		}
		for(Semaphore s : semaphores)
			try {
				s.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		int port = Integer.parseInt(cluster.get(id).split(":")[1]);
		manager.waitForConnection(port, new ConnectionEstablishmentListener());
		System.out.println("Waiting for clients at port "+port);
		System.out.println("Fusion backup repository started! Node size: "+nodeSize+", volume: "+volume+", id: "+id);
	}
	
	/**
	 * Put a data of a primary into this repository, this method will create a new entry or update existing entry
	 * @param key the key 
	 * @param prev the previous data, null if no previous data
	 * @param cur the current data
	 * @param primaryId the id of the primary
	 */
	private void putData(Serializable key, ArrayList<Double> prev, ArrayList<Double> cur, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(aux.containsKey(key))
				aux.get(key).fusedNode.updateData(covertToDataVector(prev), covertToDataVector(cur), primaryId);
			else{
				FusedNode fnode = null;
				FusedNode end = aux.size() > 0? dataStack.get(aux.size()-1): lockNode;
				end.lock();
				try{
					fnode = dataStack.get(end.id+1);
					fnode.lock();
					if(fnode.fusedNodeNumber() == 0){
						fnode.unlock();
						fnode = null;
					}
				}catch(IndexOutOfBoundsException e){}
				if(fnode == null){
					fnode = new FusedNode(nodeSize, fuseVector, dataStack.size());
					fnode.lock();
					dataStack.add(fnode);
				}
				end.unlock();
				AuxiliaryNode anode = null;
				fnode.updateData(new DoubleMatrix(nodeSize), covertToDataVector(cur), primaryId);
				anode = new AuxiliaryNode(fnode);
				fnode.setAuxiliaryNode(anode, primaryId);
				fnode.unlock();
				aux.put(key, anode);
			}
		}catch(Exception e){
			e.printStackTrace();
			throw new BackupFailureException("Operation fails! Caused by "+e);
		}
	}
	
	/**
	 * Remove a data of a primary from this repository.
	 * @param key The key to remove.
	 * @param val The current value associated with key.
	 * @param end The value of end node of this primary.
	 * @param primaryId The id of the primary
	 */
	private void removeData(Serializable key, ArrayList<Double> val, ArrayList<Double> end, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(!aux.containsKey(key))
				throw new BackupFailureException("Key does not exist! Operation cannot be completed!");
			FusedNode endNode = dataStack.get(aux.size()-1);
			AuxiliaryNode anode = aux.remove(key);
			assert(anode != null);
			anode.fusedNode.lock();
			anode.fusedNode.updateData(covertToDataVector(val), covertToDataVector(end), primaryId);
			anode.fusedNode.setAuxiliaryNode(endNode.getAuxiliaryNode(primaryId), primaryId);
			anode.fusedNode.unlock();
			endNode.getAuxiliaryNode(primaryId).fusedNode = anode.fusedNode;
			endNode.lock();
			endNode.updateData(covertToDataVector(end), new DoubleMatrix(nodeSize), primaryId);
			endNode.setAuxiliaryNode(null, primaryId);
			if(endNode.fusedNodeNumber() == 0){
				assert(dataStack.get(dataStack.size()-1) == endNode);
				dataStack.remove(dataStack.size()-1);
			}
			endNode.unlock();
		}catch(Exception e){
			e.printStackTrace();
			throw new BackupFailureException("Operation fails! Caused by "+e);
		}
	}
	
	
	
	private AuxiliaryDataStructure<Serializable> getOrCreateAuxiliaryDataStructure(int primaryId){
		AuxiliaryDataStructure<Serializable> ret = auxDataStructures[primaryId];
		if(ret == null){
			ret = new AuxiliaryHashMap<Serializable>();
			auxDataStructures[primaryId] = ret;
		}
		return ret;
	}
	
	private DoubleMatrix generateVandermondeMatrix(int m, int n){
		DoubleMatrix vander = new DoubleMatrix(m, n);
		DoubleMatrix vector = DoubleMatrix.ones(m);
		DoubleMatrix mul = DoubleMatrix.linspace(1, m, m);
		for(int i=0; i<n; i++){
			vander.putColumn(i, vector);
			vector = vector.mul(mul);
		}
		return vander;
	}
	
	private DoubleMatrix generateFuseVector(){
		DoubleMatrix vander = generateVandermondeMatrix(volume, volume+id+1);
		return Solve.solve(vander.get(new IntervalRange(0, volume), new IntervalRange(0, volume)), vander.getColumn(volume+id));
	}
	
	private DoubleMatrix covertToDataVector(ArrayList<Double> data){
		if(data == null || data.size() > nodeSize)
			throw new IllegalArgumentException("Illegal list received "+data);
		DoubleMatrix ret = DoubleMatrix.zeros(nodeSize);
		for(int i=0; i<data.size(); i++)
			ret.put(i, data.get(i));
		return ret;
	}
	
	private ArrayList<Double> convertToArrayList(DoubleMatrix m){
		assert(m != null);
		assert(m.length == nodeSize);
		assert(m.isVector());
		ArrayList<Double> ret = new ArrayList<Double>();
		for(int i=0; i<m.length; i++)
			ret.add(m.get(i));
		return ret;
	}
	
	private LinkedList<DataEntry<Serializable, ArrayList<Double>>> getData(int primaryId) throws RecoverFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primary id "+primaryId);
			if(auxDataStructures[primaryId] == null)
				throw new RecoverFailureException("Data of primary "+primaryId+" is not in this repository!");
			ArrayList<DoubleMatrix> matrices = new ArrayList<DoubleMatrix>();
			DoubleMatrix recoverVector = getEncodedmatricesAndRecoverVector(matrices, primaryId);
			assert(matrices.size() == dataStack.size());
			assert(recoverVector != null && recoverVector.length == volume);
			LinkedList<DataEntry<Serializable, ArrayList<Double>>> ret = new LinkedList<DataEntry<Serializable, ArrayList<Double>>>();
			for(DataEntry<Serializable, AuxiliaryNode> entry : auxDataStructures[primaryId].entries()){
				DoubleMatrix decodedData = matrices.get(entry.value.fusedNode.id).mmul(recoverVector);
				ret.add(new DataEntry<Serializable, ArrayList<Double>>(entry.key, convertToArrayList(decodedData)));
			}
			return ret;
		}catch(Exception e){
			e.printStackTrace();
			throw new RecoverFailureException("Unable to recover data, due to "+e);
		}
	}
	
	private boolean hasData(int primaryId){
		if(primaryId >= volume || primaryId < 0)
			throw new IllegalArgumentException("Illegal primary id "+primaryId);
		return !(auxDataStructures[primaryId] == null || auxDataStructures[primaryId].size() == 0);
	}
	
	private DoubleMatrix getEncodedmatricesAndRecoverVector(ArrayList<DoubleMatrix> matrices, int primary) throws RecoverFailureException{
		clientmapLock.readerLock();
		repomapLock.readerLock();
		try {
			LinkedList<Integer> empties = new LinkedList<Integer>();
			int columns = 0;
			for(int i=0; i<volume; i++)
				if(!hasData(i)){
					columns++;
					empties.add(i);
				}
				else if(clientid2connection.containsKey(i))
					columns++;
			columns += repoid2connection.size();
			if(columns < volume)
				throw new RecoverFailureException("Only "+columns+" columns can be collected! Unable to recover!");
			//First round, pause all operations
			LinkedList<Semaphore> semaphores = new LinkedList<Semaphore>();
			final Thread waitThread = Thread.currentThread();
			for(final Integer connection: connection2clientid.keySet()){
				final Semaphore s = new Semaphore(0);
				semaphores.add(s);
				manager.sendMessageForResponse(connection, 
						new Message().put("MessageType", MessageType.PAUSE_UPDATE),
						new MessageFilter(){
					@Override
					public boolean filter(Message msg) {
						return msg!=null && msg.containsKey("MessageType") 
								&& msg.get("MessageType") == MessageType.UPDATE_PAUSED;
					}
				}, 5000, new OnMessageReceivedListener(){
					@Override
					public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
						s.release();
					}
					@Override
					public void OnReceiveError(CommunicationManager manager, int id) {
						System.err.println("Unable to receive UPDATE_PAUSED from connection "+connection);
						waitThread.interrupt();
					}	
				}, false);
			}
			for(Semaphore s : semaphores)
				s.acquire();
			final ArrayList<HashMap<Integer, DoubleMatrix>> rawData = new ArrayList<HashMap<Integer, DoubleMatrix>>();
			for(FusedNode fnode: dataStack){
				HashMap<Integer, DoubleMatrix> node = new HashMap<Integer, DoubleMatrix>();
				node.put(id+volume, fnode.getFusedData());
				rawData.add(node);
			}
			final LinkedList<Integer> columnQueue = new LinkedList<Integer>();
			columnQueue.add(id+volume);
			columnQueue.addAll(empties);
			//Second round, require data from these primaries
			semaphores = new LinkedList<Semaphore>();
			for(Map.Entry<Integer, Integer> entry: clientid2connection.entrySet()){
				final int primaryId = entry.getKey();
				if(!hasData(primaryId)) continue;
				final int connection = entry.getValue();
				final Semaphore s = new Semaphore(0);
				semaphores.add(s);
				manager.sendMessageForResponse(connection, 
						new Message().put("MessageType", MessageType.DATA_REQUEST),
						new MessageFilter(){
					@Override
					public boolean filter(Message msg) {
						return msg!=null && msg.containsKey("MessageType") 
								&& msg.get("MessageType") == MessageType.DATA_RESPONSE;
					}
				}, 5000, new OnMessageReceivedListener(){
					@SuppressWarnings("unchecked")
					@Override
					public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
						ArrayList<ArrayList<Double>> data = (ArrayList<ArrayList<Double>>) msg.get("data");
						assert(data != null);
						if(data.size() == auxDataStructures[primaryId].size()){
							synchronized(columnQueue){
								if(columnQueue.size() == volume){
									s.release();
									return;
								}
								columnQueue.add(primaryId);
							}
							for(int i=0; i<data.size(); i++){
								HashMap<Integer, DoubleMatrix> node = rawData.get(i);
								node.put(primaryId, covertToDataVector(data.get(i)));
							}
						}
						s.release();
					}
					@Override
					public void OnReceiveError(CommunicationManager manager, int id) {
						System.err.println("Unable to receive DATA_RESPONSE from connection "+connection);
						s.release();
					}	
				}, false);
			}
			for(Semaphore s : semaphores)
				s.acquire();

			if(columnQueue.size() < volume){
				//Third Round, get data from repositories
				semaphores = new LinkedList<Semaphore>();
				for(Map.Entry<Integer, Integer> entry: repoid2connection.entrySet()){
					final int repoId = entry.getKey();
					final int connection = entry.getValue();
					final Semaphore s = new Semaphore(0);
					semaphores.add(s);
					manager.sendMessageForResponse(connection, 
							new Message().put("MessageType", MessageType.DATA_REQUEST),
							new MessageFilter(){
						@Override
						public boolean filter(Message msg) {
							return msg!=null && msg.containsKey("MessageType") 
									&& msg.get("MessageType") == MessageType.DATA_RESPONSE;
						}
					}, 5000, new OnMessageReceivedListener(){
						@SuppressWarnings("unchecked")
						@Override
						public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
							ArrayList<ArrayList<Double>> data = (ArrayList<ArrayList<Double>>) msg.get("data");
							assert(data != null);
							if(data.size() == dataStack.size()){
								synchronized(columnQueue){
									if(columnQueue.size() == volume){
										s.release();
										return;
									}
									columnQueue.add(repoId+volume);
								}
								for(int i=0; i<data.size(); i++){
									HashMap<Integer, DoubleMatrix> node = rawData.get(i);
									node.put(repoId+volume, covertToDataVector(data.get(i)));
								}
							}
							s.release();
						}
						@Override
						public void OnReceiveError(CommunicationManager manager, int id) {
							System.err.println("Unable to receive DATA_RESPONSE from connection "+connection);
							s.release();
						}	
					}, false);
				}
				for(Semaphore s : semaphores)
					s.acquire();
			}

			//Generate data matrices
			if(columnQueue.size() < volume)
				throw new RecoverFailureException("Unable to collect enough columns! Only "+columnQueue.size()+" columns got!");
			assert(columnQueue.size() == volume);
			Collections.sort(columnQueue);
			for(HashMap<Integer, DoubleMatrix> node: rawData){
				DoubleMatrix matrix = new DoubleMatrix(nodeSize, volume);
				for(int i=0; i<columnQueue.size(); i++){
					int column = columnQueue.get(i);
					if(node.containsKey(column))
						matrix.putColumn(i, node.get(column));
				}
				matrices.add(matrix);
			}							

			DoubleMatrix vander = generateVandermondeMatrix(volume, Collections.max(columnQueue)+1);
			DoubleMatrix A = vander.get(new IntervalRange(0, vander.rows), new IntervalRange(0, vander.rows));
			DoubleMatrix B = vander.get(new IntervalRange(0, vander.rows), new IntervalRange(vander.rows, vander.columns));
			DoubleMatrix X = Solve.solve(A, B);
			DoubleMatrix Convert = DoubleMatrix.concatHorizontally(DoubleMatrix.eye(vander.rows), X);
			int[] indices = new int[columnQueue.size()];
			for(int i=0; i<indices.length; i++)
				indices[i] = columnQueue.get(i);
			Convert = Convert.get(new IntervalRange(0, Convert.rows), new IndicesRange(indices));
			assert(Convert.rows == Convert.columns);
			return Solve.pinv(Convert).getColumn(primary);
		} catch (Exception e) {
			throw new RecoverFailureException("Unable to get encoded matrix due to "+e);
		} finally{
			for(final Integer connection: connection2clientid.keySet()){
				try {
					manager.sendMessage(connection, new Message().put("MessageType", MessageType.RESUME_UPDATE));
				} catch (IOException e) {
					System.err.println("Error! Unable to send RESUME_UPDATE!");
				}
			}
			repomapLock.readerUnlock();	
			clientmapLock.readerUnlock();
		}

	}
	
	private class ClusterMessageListener implements OnMessageReceivedListener{

		@Override
		public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
			assert(msg.containsKey("MessageType"));
			repomapLock.readerLock();
			final int nodeId = connection2repoid.get(id);
			repomapLock.readerUnlock();
			try {
				switch((MessageType)msg.get("MessageType")){
					case DATA_REQUEST:
						ArrayList<ArrayList<Double>> data = new ArrayList<ArrayList<Double>>();
						for(FusedNode node: dataStack){
							data.add(convertToArrayList(node.getFusedData()));
						}
						manager.sendMessage(id, new Message().put("MessageType", MessageType.DATA_RESPONSE).
															  put("data", data));
						break;
					case EXCEPTION:
						System.err.println("ClusterMessageListener: Cluster "+nodeId+" has internal error: "+msg.get("Exception"));
					default:
						break;
				}
			}catch(Exception e){
				e.printStackTrace();
				try {
					manager.sendMessage(id, new Message().put("MessageType", MessageType.EXCEPTION).put("Exception", e));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("ClusterMessageListener: Error occurs when receiving message!");
			manager.closeConnection(id);
			repomapLock.writerLock();
			int repoId = connection2repoid.remove(id);
			repoid2connection.remove(repoId);
			repomapLock.writerUnlock();
		}
		
	}
	
	private class ClientMessageListener implements OnMessageReceivedListener{
		
		@SuppressWarnings("unchecked")
		@Override
		public void OnMessageReceived(final CommunicationManager manager, final int id, Message msg) {
			assert(msg.containsKey("MessageType"));
			clientmapLock.readerLock();
			final int clientId = connection2clientid.get(id);
			clientmapLock.readerUnlock();
			try {
				switch((MessageType)msg.get("MessageType")){
					case BACKUP_UPDATE:
						switch((UpdateType)msg.get("UpdateType")){
							case PUT:
								Serializable key = msg.get("key");
								ArrayList<Double> prev = (ArrayList<Double>)msg.get("prev");
								ArrayList<Double> cur = (ArrayList<Double>)msg.get("cur");
								putData(key, prev, cur, clientId);
								break;
							case REMOVE:
								key = msg.get("key");
								ArrayList<Double> val = (ArrayList<Double>)msg.get("val");
								ArrayList<Double> end = (ArrayList<Double>)msg.get("end");
								removeData(key, val, end, clientId);
								break;
						}
						break;
					case RECOVER_REQUEST:						
						if(!hasData(clientId)){
							Message reply = new Message().put("MessageType", MessageType.RECOVER_RESULT);
							reply.put("success", true).
								  put("result", new LinkedList<DataEntry<Serializable, ArrayList<Double>>>());
							manager.sendMessage(id, reply);
						}else{
							new Thread(){
								@Override
								public void run(){
									Message reply = new Message().put("MessageType", MessageType.RECOVER_RESULT);
									try{
										LinkedList<DataEntry<Serializable, ArrayList<Double>>> result = getData(clientId);
										reply.put("success", true).
											  put("result", result);
									}catch(RecoverFailureException e){
										e.printStackTrace();
										reply.put("success", false);
									}
									try {
										manager.sendMessage(id, reply);
									} catch (IOException e) {
										System.err.println("Unable to send RECOVER_RESULT");
									}
								}
							}.start();
						}
						break;
					case EXCEPTION:
						System.err.println("ClientMessageListener: Client "+clientId+" has internal error: "+msg.get("Exception"));
					default:
						break;
				}
			}catch(Exception e){
				e.printStackTrace();
				try {
					manager.sendMessage(id, new Message().put("MessageType", MessageType.EXCEPTION).put("Exception", e));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("ClientMessageListener: Error occurs when receiving message!");
			manager.closeConnection(id);
			clientmapLock.writerLock();
			int clientId = connection2clientid.remove(id);
			clientid2connection.remove(clientId);
			clientmapLock.writerUnlock();
		}
		
	}
	
	private class ConnectionEstablishmentListener implements OnConnectionListener{
		@Override
		public void OnConnected(CommunicationManager manager, int id) {
			manager.setOnMessageReceivedListener(id, new OnMessageReceivedListener(){
				@Override
				public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
					if(msg.get("MessageType") != MessageType.CONNECT_REQUEST) return;
					int nodeId = (Integer) msg.get("id");
					NodeType type = (NodeType) msg.get("NodeType");
					boolean success = false;
					if(type == NodeType.CLIENT){
						clientmapLock.writerLock();
						if(nodeId<0 || nodeId>=volume || clientid2connection.containsKey(nodeId)){
							manager.closeConnection(id);
							System.err.println("ConnectionEstablishmentListener: illegal or duplicated client id "+nodeId+", rejected!");
						}else{
							clientid2connection.put(nodeId, id);
							connection2clientid.put(id, nodeId);
							manager.setOnMessageReceivedListener(id, new ClientMessageListener());
							System.out.println("ConnectionEstablishmentListener: Connected to client "+nodeId);
							success = true;
						}
						clientmapLock.writerUnlock();
					}else{
						repomapLock.writerLock();
						if(repoid2connection.containsKey(nodeId)){
							manager.closeConnection(id);
							System.err.println("ConnectionEstablishmentListener: duplicate connect from repository "+nodeId+", rejected!");
						}else{
							repoid2connection.put(nodeId, id);
							connection2repoid.put(id, nodeId);
							manager.setOnMessageReceivedListener(id, new ClusterMessageListener());
							System.out.println("ConnectionEstablishmentListener: Connected to repository "+nodeId);
							success = true;
						}
						repomapLock.writerUnlock();
					}
					if(!success) return;
					try {
						manager.sendMessage(id, new Message().put("MessageType", MessageType.CONNECT_ACCEPTED));
					} catch (IOException e) {
						e.printStackTrace();
						clientmapLock.writerLock();
						if(connection2clientid.containsKey(id)){
							connection2clientid.remove(id);
							clientid2connection.remove(nodeId);
						}
						clientmapLock.writerUnlock();
						repomapLock.writerLock();
						if(connection2repoid.containsKey(id)){
							connection2repoid.remove(id);
							repoid2connection.remove(nodeId);
						}
						repomapLock.writerUnlock();
						System.err.println("ConnectionEstablishmentListener: Unable to send CONNECT_ACCEPTED!");
						manager.closeConnection(id);
					}
				}
				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					manager.closeConnection(id);
				}	
			});
		}

		@Override
		public void OnConnectFail(CommunicationManager manager) {
			System.err.println("ConnectionEstablishmentListener: Error occurs when establishing connection!");
		}
		
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder("{\n");
		str.append("id: "+id+"\n");
		str.append("node size: "+nodeSize+"\n");
		str.append("volume: "+volume+"\n");
		str.append("Fuse vector: "+fuseVector+"\n");
		str.append("dataStack:\n  [\n");
		for(FusedNode node : dataStack)
			str.append("    "+node+"\n");
		str.append("  ]\n");
		str.append("Auxiliary data structures:\n  [\n");
		for(AuxiliaryDataStructure<Serializable> aux : auxDataStructures)
			str.append("    "+aux+"\n");
		str.append("  ]\n");
		str.append("}\n");
		return str.toString();
	}
	
	public static void main(String[] args) throws Exception{
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		FusedRepository repo0 = new FusedRepository(2, 4, 0, cluster);
		FusedRepository repo1 = new FusedRepository(2, 4, 1, cluster);
//		FusedRepository repo2 = new FusedRepository(2, 4, 2, cluster);
//		FusedRepository repo3 = new FusedRepository(2, 4, 3, cluster);
//		FusedRepository repo4 = new FusedRepository(2, 4, 4, cluster);
	}
}
