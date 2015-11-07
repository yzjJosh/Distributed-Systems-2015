package backups;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;

import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import communication.CommunicationManager;
import communication.Message;
import communication.MessageFilter;
import communication.OnConnectionListener;
import communication.OnMessageReceivedListener;
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
	private final HashMap<Integer, Integer> id2conncetion;
	private final HashMap<Integer, Integer> connection2id;
	private final Object mapLock = new Object();
	
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
		this.id2conncetion = new HashMap<Integer, Integer>();
		this.connection2id = new HashMap<Integer, Integer>();
		for(Map.Entry<Integer, String> entry : cluster.entrySet()){
			int nodeId = entry.getKey();
			if(nodeId == id) continue;
			String[] temp = entry.getValue().split(":");
			String ip = temp[0];
			int port = Integer.parseInt(temp[1]);
			try {
				int connection = manager.connect(ip, port);		
				manager.setOnMessageReceivedListener(connection, new ClusterMessageListener(nodeId));		
			} catch (IOException e) {}
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
	public void putData(Serializable key, ArrayList<Double> prev, ArrayList<Double> cur, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(aux.containsKey(key))
				aux.get(key).fusedNode.updateData(covertToDataVector(prev), covertToDataVector(cur), primaryId);
			else{
				FusedNode fnode = null;
				if(dataStack.size() > aux.size())
					fnode = dataStack.get(aux.size());
				else{
					fnode = new FusedNode(nodeSize, fuseVector, dataStack.size());
					dataStack.add(fnode);
				}
				fnode.updateData(covertToDataVector(prev), covertToDataVector(cur), primaryId);
				AuxiliaryNode anode = new AuxiliaryNode(fnode);
				fnode.setAuxiliaryNode(anode, primaryId);
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
	public void removeData(Serializable key, ArrayList<Double> val, ArrayList<Double> end, int primaryId) throws BackupFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primaryId "+primaryId);
			AuxiliaryDataStructure<Serializable> aux = getOrCreateAuxiliaryDataStructure(primaryId);
			if(!aux.containsKey(key))
				throw new BackupFailureException("Key does not exist! Operation cannot be completed!");
			FusedNode endNode = dataStack.get(aux.size()-1);
			AuxiliaryNode anode = aux.remove(key);
			assert(anode != null);
			anode.fusedNode.updateData(covertToDataVector(val), covertToDataVector(end), primaryId);
			endNode.updateData(covertToDataVector(end), covertToDataVector(null), primaryId);
			endNode.getAuxiliaryNode(primaryId).fusedNode = anode.fusedNode;
			anode.fusedNode.setAuxiliaryNode(endNode.getAuxiliaryNode(primaryId), primaryId);
			endNode.setAuxiliaryNode(null, primaryId);
			if(endNode.fusedNodeNumber() == 0){
				assert(dataStack.get(dataStack.size()-1) == endNode);
				dataStack.remove(dataStack.size()-1);
			}
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
	
	private DoubleMatrix generateFuseVector(){
		int n = volume;
		DoubleMatrix A = new DoubleMatrix(n, n);
		DoubleMatrix vector = DoubleMatrix.ones(n);
		DoubleMatrix mul = DoubleMatrix.linspace(1, n, n);
		for(int i=0; i<n; i++){
			A.putColumn(i, vector);
			vector = vector.mul(mul);
		}
		for(int i=0; i<id; i++)
			vector = vector.mul(mul);
		return Solve.solve(A, vector);
	}
	
	private DoubleMatrix covertToDataVector(ArrayList<Double> data){
		if(data != null && data.size() > nodeSize)
			throw new IllegalArgumentException("Data size "+data.size()+" exceeds node size "+nodeSize);
		DoubleMatrix ret = DoubleMatrix.zeros(nodeSize);
		if(data != null)
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
	
	public LinkedList<DataEntry<Serializable, ArrayList<Double>>> getData(int primaryId) throws RecoverFailureException{
		try{
			if(primaryId >= volume || primaryId < 0)
				throw new IllegalArgumentException("Illegal primary id "+primaryId);
			if(auxDataStructures[primaryId] == null)
				throw new RecoverFailureException("Data of primary "+primaryId+" is not in this repository!");
			ArrayList<DoubleMatrix> matrixes = getEncodedMatrixes();
			assert(matrixes != null && matrixes.size() == dataStack.size());
			DoubleMatrix recoverVector = getRecoverVector(primaryId);
			assert(recoverVector != null && recoverVector.length == volume);
			LinkedList<DataEntry<Serializable, ArrayList<Double>>> ret = new LinkedList<DataEntry<Serializable, ArrayList<Double>>>();
			for(DataEntry<Serializable, AuxiliaryNode> entry : auxDataStructures[primaryId].entries()){
				DoubleMatrix decodedData = matrixes.get(entry.value.fusedNode.id).mmul(recoverVector);
				ret.add(new DataEntry<Serializable, ArrayList<Double>>(entry.key, convertToArrayList(decodedData)));
			}
			return ret;
		}catch(Exception e){
			e.printStackTrace();
			throw new RecoverFailureException("Unable to recover data, due to "+e);
		}
	}
	
	private ArrayList<DoubleMatrix> getEncodedMatrixes(){
		ArrayList<DoubleMatrix> ret= new ArrayList<DoubleMatrix>();
		DoubleMatrix mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(0, new DoubleMatrix(new double[]{1.0, 5.5}));
		mat.putColumn(volume-1, dataStack.get(0).getFusedData());
		ret.add(mat);
		mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(0, new DoubleMatrix(new double[]{2.0, 3.5}));
		mat.putColumn(volume-1, dataStack.get(1).getFusedData());
		ret.add(mat);
		mat = new DoubleMatrix(nodeSize, volume);
		mat.putColumn(volume-1, dataStack.get(2).getFusedData());
		ret.add(mat);
		return ret;
	}
	
	private DoubleMatrix getRecoverVector(int primaryId){
		return Solve.pinv(DoubleMatrix.concatHorizontally(DoubleMatrix.eye(volume).get(new int[]{0,1,2,3}, new int[]{1,2,3}), fuseVector)).getColumn(primaryId);
	}
	
	private class ClusterMessageListener implements OnMessageReceivedListener{
		
		private final int clusterId;
		
		public ClusterMessageListener(int clusterId){
			this.clusterId = clusterId;
		}

		@Override
		public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
			if(!msg.containsKey("MessageType")) return;
			try {
				switch((MessageType)msg.get("MessageType")){
					case INFO_REQUEST:
						manager.sendMessageForResponse(id, new Message().
								put("MessageType", MessageType.INFO_RESPONSE).
								put("NodeType", NodeType.REPOSITORY).
								put("id", FusedRepository.this.id),
								new MessageFilter(){
									@Override
									public boolean filter(Message msg) {
										return msg != null && msg.containsKey("MessageType") && msg.get("MessageType") == MessageType.INFO_RESPONSE_ACK;
									}	
								}, 5000, new OnMessageReceivedListener(){

									@Override
									public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
										synchronized(mapLock){
											id2conncetion.put(clusterId, id);
											connection2id.put(id, clusterId);
										}
										System.out.println("Connected to repository node "+clusterId);
									}

									@Override
									public void OnReceiveError(CommunicationManager manager, int id) {
										System.err.println("ClusterMessageListener: Error! Unable to receive INFO_RESPONSE_ACK!");
										manager.closeConnection(id);
									}
									
								}, false);
						break;
					default:
						break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("ClusterMessageListener: Error occurs when receiving message!");
			manager.closeConnection(id);
		}
		
	}
	
	private class ClientMessageListener implements OnMessageReceivedListener{
		
		
		@Override
		public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
			
		}

		@Override
		public void OnReceiveError(CommunicationManager manager, int id) {
			System.err.println("ClientMessageListener: Error occurs when receiving message!");
			manager.closeConnection(id);
		}
		
	}
	
	private class ConnectionEstablishmentListener implements OnConnectionListener{

		@Override
		public void OnConnected(CommunicationManager manager, int id) {
			try {
				manager.sendMessageForResponse(id, new Message().put("MessageType", MessageType.INFO_REQUEST), 
						new MessageFilter(){
							@Override
							public boolean filter(Message msg) {
								return msg != null && msg.containsKey("MessageType") && msg.get("MessageType") == MessageType.INFO_RESPONSE;
							}
				}, 5000, new OnMessageReceivedListener(){

					@Override
					public void OnMessageReceived(CommunicationManager manager, int id, Message msg) {
						try {
							manager.sendMessage(id, new Message().put("MessageType", MessageType.INFO_RESPONSE_ACK));
						} catch (IOException e) {
							e.printStackTrace();
							System.err.println("ConnectionEstablishmentListener: Error! Unable to send INFO_RESPONSE_ACK!");
							manager.closeConnection(id);
							return;
						}
						if(msg.get("NodeType") == NodeType.REPOSITORY){
							int repoId = (Integer)msg.get("id");
							synchronized(mapLock){
								id2conncetion.put(repoId, id);
								connection2id.put(id, repoId);
							}
							manager.setOnMessageReceivedListener(id, new ClusterMessageListener(repoId));
							System.out.println("Connected to repository node "+repoId);
						}else{
							int primaryId = (Integer)msg.get("id");
							manager.setOnMessageReceivedListener(id, new ClientMessageListener());
							System.out.println("Connected to client node "+primaryId);
						}
					}

					@Override
					public void OnReceiveError(CommunicationManager manager, int id) {
						System.err.println("ConnectionEstablishmentListener: Error! Unable to receive INFO_RESPONSE!");
						manager.closeConnection(id);
					}
					
				}, false);
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("ConnectionEstablishmentListener: Error! Unable to send INFO_REQUEST!");
				manager.closeConnection(id);
			}
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
	
	public static void main(String[] args) throws UnknownHostException{
		HashMap<Integer, String> cluster = new HashMap<Integer, String>();
		String ip = Inet4Address.getLocalHost().getHostAddress();
		cluster.put(0, ip+":12345");
		cluster.put(1, ip+":12346");
		cluster.put(2, ip+":12347");
		cluster.put(3, ip+":12348");
		cluster.put(4, ip+":12349");
		FusedRepository repo0 = new FusedRepository(2, 4, 0, cluster);
		FusedRepository repo1 = new FusedRepository(2, 4, 1, cluster);
		FusedRepository repo2 = new FusedRepository(2, 4, 2, cluster);
		FusedRepository repo3 = new FusedRepository(2, 4, 3, cluster);
		FusedRepository repo4 = new FusedRepository(2, 4, 4, cluster);
	}
}
