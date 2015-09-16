package server;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * A server process in a distributed system.
 */
public class Server {
	
	private static Clock clock; //The Lamport's logical clock.
	private static int pid;		//The pid of current process.
	private static final HashMap<Integer, ServerState> clusterInfo = new HashMap<Integer, ServerState>(); //Pid to every srever's state in the cluster.
	private static final PriorityQueue<Message> waitingQueue = new PriorityQueue<Message>();		  //The queue of waiting requests
	private static final LinkedList<Message> inCriticalSection = new LinkedList<Message>();	  //Requests that are currently in critical section
	
	//Synchronization locks
	private static Object clock_lock = new Object();	//clock access mutex lock
	
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
	 * Update the logical clock, increase the timestep of this process by 1.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(){
		synchronized(clock_lock){
			//Enter critical section.
			clock = new Clock(clock.timeStep+1, clock.pid);
			//Release critical section.
		}
		return clock;
	}
	
	/**
	 * Update the logical clock of this process according to a recerived timestep.
	 * @param timestep The timestep of a message.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(Clock timestep){
		synchronized(clock_lock){
			//Enter critical section.
			clock = new Clock(Math.max(clock.timeStep, timestep.timeStep)+1, clock.pid);
			//Release critical section.
		}
		return clock;
	}

	/**
	 * Send a message and update the clock(increase the timestep by 1) at the same time.
	 * @param msg The message to send.
	 * @param socket The socket to send that message.
	 * @throws IOException If there is an io error when sending message.
	 */
	private static void sendMessage(Message msg, Socket socket) throws IOException{
		updateClock();
		new ObjectOutputStream(socket.getOutputStream()).writeObject(msg);
	}
	
	/**
	 * Wait for a message from a socket for certain number of time. This method is useful to check if a server is dead.
	 * @param socket The socket to wait message on.
	 * @param waitTime The time of waiting. If no message received after that time, an IOException will be thrown.
	 * @return Received message.
	 * @throws IOException If there is an io error, or does not hear back on time.
	 */
	private static Message waitForMessage(Socket socket, final int waitTime) throws IOException{
		final ObjectInputStream istream = new ObjectInputStream(socket.getInputStream()); //Get inputstream from socket.
		Thread monitor = new Thread(new Runnable(){ // Create a new thread, wait some time and shut down the stream.
			@Override
			public void run() {
				try {
					Thread.sleep(waitTime);
					istream.close();
				} catch (InterruptedException e) {}
				catch(IOException e){}	
			}
		});
		monitor.start(); // Start the new thread.
		Message msg = null;
		try {
			msg = (Message) istream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		if(msg.content instanceof Clock) updateClock((Clock) msg.content); //Update the clock.
		monitor.interrupt(); // Stop the monitor from waiting.
		return msg;
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
		if(msg.content instanceof Clock) updateClock((Clock) msg.content); //Update the clock firstly.
		
		
	}
	
	/**
	 * Send the timesteped message to all other servers.
	 */
	public static void broadCastClock(){
		for(ServerState serverstat : clusterInfo.values()){
			if(!serverstat.live || serverstat.pid == pid) continue;
			try {
				Socket socket = new Socket(serverstat.ipAddress, serverstat.port);
				updateClock();
				new ObjectOutputStream(socket.getOutputStream()).writeObject(clock);
				socket.close();
			} catch (UnknownHostException e) {
			} catch (IOException e) {
			}
		}
	}
	
	/**
	 * Entrance of the server process.
	 * @param args args[0] is the file where the server addresses and port# are defined.
	 */
	public static void main(String[] args){
		try {
			Server.init(args[0]);
			new ClockUpdateThread(5000).start();
			ServerSocket serversocket = new ServerSocket(clusterInfo.get(pid).port);
			while(true)
				new ServerThread(serversocket.accept()).start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
