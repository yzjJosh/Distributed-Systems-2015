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
	private static final PriorityQueue<Message> readRequests = new PriorityQueue<Message>();		  //The queue of waiting read requests
	private static final PriorityQueue<Message> writeRequests = new PriorityQueue<Message>();	  		//The queue of waiting write requests
	
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
	private static void requestCritialSection(Message message) throws IOException {
		// If the process is the first one in the reader queue
		if (message.type == MessageType.CS_REQUEST_READ) {
			while(!writeRequests.isEmpty()){        //writeRequest queue is not empty, so it has to wait
				try {
					Thread.currentThread();
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return;              //After it's notified and satisfies the requirements, it can enter the cs.
		}else if (message.type == MessageType.CS_REQUEST_WRITE && pid == writeRequests.peek().clk.pid) {  //it's a write thread and is the first thread in the queue
			if(readRequests.isEmpty()){                 //The read queue is empty so it can directly enter the cs.
				return;
			}
			while(message.clk.timestamp >= readRequests.peek().clk.timestamp){   //If its timestamp is larger than or equal to the first read thread, it's hung up
				try {
					Thread.currentThread();
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return;         //After it's notified and satisfies the requirements, it can enter the cs.
		}
	}
	
	/**
	 * Release the critial section, so that other server processes can enter the critial section.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void releaseCritialSection(Message message) throws IOException{
		if(message.type == MessageType.CS_REQUEST_READ) {
			readRequests.poll();
		}else {
			writeRequests.poll();
		}
		
	}
	
	/**
	 * Update the logical clock, increase the timestamp of this process by 1.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(){
		synchronized(clock_lock){
			//Enter critical section.
			clock = new Clock(clock.timestamp+1, clock.pid);
			
			//Release critical section.
		}
		return clock;
	}
	
	/**
	 * Update the logical clock of this process according to a recerived timestamp.
	 * @param timestamp The timestamp of a message.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(Clock timestamp){
		if(timestamp == null) return clock;
		synchronized(clock_lock){
			//Enter critical section.
			clock = new Clock(Math.max(clock.timestamp, timestamp.timestamp)+1, clock.pid);
			//Release critical section.
		}
		return clock;
	}

	/**
	 * Send a timestped message through a socket, and update the clock at the same time.
	 * @param socket The socket to send message
	 * @param type The type of message
	 * @param content The content of message
	 * @throws IOException If some io errors occur
	 */
	private static void sendMessage(Socket socket, MessageType type, Serializable content) throws IOException{
		new ObjectOutputStream(socket.getOutputStream()).writeObject(new Message(type, content, updateClock()));
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
		updateClock(msg.clk); //Update the clock.
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
		updateClock(msg.clk); //Update the clock firstly.
		switch(msg.type) {      //Add the message into the corresponding queue.
		case ACKNOWLEDGE_READ: 
			readRequests.add(msg);
		case ACKNOWLEDGE_WRITE:
			writeRequests.add(msg);
		}
		
		
	}
	
	/**
	 * Send the timestamped message to all other servers.
	 */
	public static void broadCastClock(){
		for(ServerState serverstat : clusterInfo.values()){
			if(!serverstat.live || serverstat.pid == pid) continue;
			try {
				Socket socket = new Socket(serverstat.ipAddress, serverstat.port);
				sendMessage(socket, MessageType.CLOCK_MESSAGE, null);
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
