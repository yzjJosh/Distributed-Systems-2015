package chord;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
/**
 * Provides all methods necessary for a user application. This includes methods
 * for changing connectivity to the network (create, join, leave) as well as for
 * working with content (insert, retrieve, remove).
 * 
 * @author 	Yu Sun
 */
public class ChordNode {
	/**
	 * The identifier of this node
	 */
	public int identifier;
	/**
	 * The node's finger table. 
	 */
	protected FingerTable fingerTable;
	/**
	 * A reference to this node's successor. 
	 */
	protected ChordNode successor;
	/**
	 * A reference to this node's predecessor. 
	 */
	protected ChordNode predecessor;
	
	private static final int MAX_RESPONSE_TIME = 5000;	//The maximum response time of this system.
	private static final HashMap<Integer, String> clusterInfo = new HashMap<Integer, String>(); //Pid to every srever's process in the cluster.
	protected List<Socket> socket = new LinkedList<Socket>(); 
	protected static ServerSocket serverSocket = null;
	public static void initConnection(String path, int maxNumOfSeates) throws IOException, FileNotFoundException {
		
		String serverInfo;
		BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
		while ((serverInfo = reader.readLine()) != null) {
			//Split the serverInfo to get the host and port.
			String[] splits = serverInfo.split(" ");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);	
			int hash = (ip + "/" + port).hashCode();
			clusterInfo.put(hash, serverInfo);
			
		}
		reader.close();
		
		//Get my pid.
		System.out.println("My IP adress is: "+InetAddress.getLocalHost().getHostAddress());
		//Write new node info into the file
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path)));
		
		//Add itself to the information sheet.
		try{
				//Specify a random port number.
				int port = (int) (10000 + Math.random() * 10000);
				String ip = InetAddress.getLocalHost().getHostAddress();
				int hash = (ip + "/" + port).hashCode();
				String data = port + " " + ip + " " + hash + "\n";
				writer.write(data);
				writer.close();
				
				serverSocket = new ServerSocket(port);
				while (true) {
					//Wait for the client to connect.
					Socket client = serverSocket.accept(); 
					new ChordNodeThread(client); 
					
				}
		}catch(IOException e){
			System.out.println(e);
		}
		
		try {
			
		} catch () {
			
		}
		
		if(serverSocket == null)
				throw new IOException("Unable to find available port!");
	}
	
	/**
	 * Find the successor node for the given identifier.  The successor
	 * node is the node responsible for storing any <key, value> pair
	 * whose key hashes to the identifier being sought.
	 * 
	 * @param id The identifier being sought
	 * 
	 * @return The node responsible for id
	 */
	public ChordNode find_successor(int id) {
		ChordNode m = find_predecessor(id);
		return m.successor;
	}
	
	public ChordNode find_predecessor(int id) {
		ChordNode m = this;
		while (id <= m.identifier || id > m.successor.identifier) {
			m = m.closest_preceding_finger(id);
		}
		return m;
	}
	
	public ChordNode closest_preceding_finger(int id) {
		for (int i = 0; i < fingerTable.size; i++) {
			if (fingerTable.finger.get(i).node > identifier && fingerTable.finger.get(i).node < id) {
				return fingerTable.finger.get(i).node;			
			}
		}
		
	}
	

	
}
