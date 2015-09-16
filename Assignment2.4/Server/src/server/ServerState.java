package server;

/**
 * ServerState defines the state of a server.
 *
 */
public class ServerState {
	public final int pid;			//The pid of a server.
	public final String ipAddress;	//The ipAddress of a server.
	public final int port;			//The port of a server.
	public boolean live;			//If the server process live or dead.
	
	/**
	 * Create a new ServerState object
	 * @param ip The ip address
	 * @param port The port
	 * @param live If the server is live or dead
	 */
	public ServerState(int pid, String ip, int port, boolean live){
		this.pid = pid;
		this.ipAddress = ip;
		this.port = port;
		this.live = live;
	}
}
