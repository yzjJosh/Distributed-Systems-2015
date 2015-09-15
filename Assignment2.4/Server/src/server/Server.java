package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * A server process in a distributed system.
 */
public class Server {
	
	private static Clock clock; //The Lamport's logical clock.
	private static int pid;		//The pid of current process.
	private static HashMap<Integer, Entry<String, Integer>> pidToAddress; //The pid to ip address and port map.
	
	/**
	 * Initialize the server process with an info file.
	 * @param infoFile The file where ips and ports are defined.
	 * @throws IOException If there is an error when reading the file.
	 */
	private static void init(String infoFile) throws IOException{
		
	}
	
	/**
	 * Request critial section access. If critial section is unavailable, block the thread until it becomes available.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void requestCritialSection() throws IOException{
		
	}
	
	/**
	 * Release the critial section, so that other server processes can enter the critial section.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void releaseCritialSection() throws IOException{
		
	}

	
	/**
	 * Update the logical clock of this process according to a recerived timestep.
	 * @param timestep The timestep of a message. If the timestep is null, this method increases the logical clock by 1.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(Clock timestep){
		return null;
	}
	
	
	/**
	 * This method is called whenever a message is received. The message could be a client request or messages from 
	 * other servers. These messages will be processed and responded here. (This method may be called by different threads
	 * simontaneously, so be careful with concurrency when implementing it)
	 * @param msg The message received.
	 * @param socket The socket where this message is sent.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	public static void onReceivingMessage(Message msg, Socket socket) throws IOException{
		
	}
	
	/**
	 * Get the Lamport's logical clock of this process.
	 * @return The current logical clock of this process.
	 */
	public static Clock getClock(){
		return clock;
	}
	
	/**
	 * Entrance of the server process.
	 * @param args args[0] is the file where the server addresses and port# are defined.
	 */
	public static void main(String[] args){
		try {
			Server.init(args[0]);
			new ClockUpdateThread(5000).start();
			ServerSocket serversocket = new ServerSocket(pidToAddress.get(pid).getValue());
			while(true)
				new ServerThread(serversocket.accept()).start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
