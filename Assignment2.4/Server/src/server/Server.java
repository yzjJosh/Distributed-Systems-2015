package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * A server process in a distributed system.
 */
public class Server {
	
	private static Clock clock; //The Lamport's logical clock.
	private static int pid;		//The pid of current process.
	private static final HashMap<Integer, ServerState> clusterInfo = new HashMap<Integer, ServerState>(); //Pid to every srever's state in the cluster.
	private static final PriorityQueue<Message> requests = new PriorityQueue<Message>();		  //The queue of waiting requests
	private static final PriorityQueue<Message> writeRequests = new PriorityQueue<Message>();	  		//The queue of waiting write requests
	
	//Synchronization locks
	private static Object clock_lock = new Object();	//clock access mutex lock
	private static final int MAX_READER_IN_A_SERVER = 20;	//Maximum number of concurent readers in each server.
	private static Semaphore read_write_lock = new Semaphore(MAX_READER_IN_A_SERVER);	//The read-write lock
	
	
	/**
	 * Initialize the server process with an info file.
	 * @param infoFile The file where ips and ports are defined.
	 * @throws IOException If there is an error when reading the file.
	 */
	private static void init(String path) throws IOException {
		try {

			BufferedReader br = new BufferedReader(new FileReader(path));
			StringBuffer sb = new StringBuffer();
			String server = br.readLine();
			while (server != null) {
				// Extract the ip and port information from the line.
				String[] splits = server.split(" ");
				String ip = splits[0];
				int port = Integer.parseInt(splits[1]);
				// Try to find out if the server is alive by sending an ack and
				// check if the sender server can receive a response in time.
				try {
					Socket socket = new Socket(ip, port);
					socket.setSoTimeout(5 * 1000); // set the timeout to 5s
					sendMessage(socket, new Message(MessageType.SERVER_SYNC, null, updateClock()));
					ObjectInputStream reader = new ObjectInputStream(
							socket.getInputStream());
					reader.readObject();
					socket.close();
					// Timeout Exception doesn't happen, so this server is
					// alive.
					ServerState state = new ServerState(pid, ip, port, true);
					clusterInfo.put(pid, state);
				} catch (UnknownHostException e) {
				} catch (SocketTimeoutException e) {
					// Timeout Exception happens, so this server is not alive
					ServerState state = new ServerState(pid, ip, port, false);
					clusterInfo.put(pid, state);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				// Read the next server information.
				br.readLine();

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Request critial section access. If critial section is unavailable, block the thread until it becomes available.
	 * @param read true if read, false if write
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void requestCritialSection(boolean read) throws IOException {
		//Acquire lock firstly
		try {
			if(read) read_write_lock.acquire();
			else read_write_lock.acquire(MAX_READER_IN_A_SERVER);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		final MessageType type = read? MessageType.CS_REQUEST_READ : MessageType.CS_REQUEST_WRITE;		//The sending message type.
		final MessageType ackType = read? MessageType.ACKNOWLEDGE_READ : MessageType.ACKNOWLEDGE_WRITE;	//The receiving message type.
		final Clock timestampOfRequest = updateClock();
		class Lock{			
			/**
			 * The lock is used for synchronization purpose.
			 * Whenever send a request, num++;
			 * Whenever receive a ack or a server is believed to be dead, num--;
			 * The main thread will wait until num==0. 
			 */
			public int num = 0;
		}
		final Lock ackLock = new Lock();
		final Thread mainThread = Thread.currentThread();
		//Send the read requests to all other servers
		synchronized(clusterInfo){ //No two threads can take clusterInfo's lock at the same time.
			for(ServerState serverstat : clusterInfo.values()){
				if(!serverstat.live || serverstat.pid == pid) continue;
				final ServerState stat = serverstat;
				synchronized(ackLock){
					ackLock.num ++;	//Send a request, lock.num++.
				}
				new Thread(){
					@Override
					public void run(){
						try {
							Socket socket = new Socket(stat.ipAddress, stat.port);
							updateClock();
							sendMessage(socket, new Message(type, null, timestampOfRequest));	//Send request to a server
							while(waitForMessage(socket, 5000).type != ackType);	//Wait for its ack reply for 5s.
							socket.close();
						} catch (UnknownHostException e) {
						} catch (IOException e) {
							synchronized(clusterInfo){
								stat.live = false;	//If no response, set it dead.
							}
						}
						//After receive the ACK or set the server dead, lock.num--.
						synchronized(ackLock){
							ackLock.num--;
							mainThread.interrupt();
						}
					}
				}.start();
			}
		}
		
		while(ackLock.num > 0)
			try {
				Thread.sleep(1000);	//If have not received enough ack, sleep.
			} catch (InterruptedException e) {}
		//---------------------------------------------------------------------------------------------------------------
		//If enter this line, then congratulations! You have received acks from all lived servers
		
		if(read){
			synchronized(requests){
				requests.add(new Message(MessageType.CS_REQUEST_READ, null, timestampOfRequest));	//Add itself to the request queue
				//If there is at least one write request whose timestamp is smaller, it has to wait
				while(!writeRequests.isEmpty() && writeRequests.peek().clk.compareTo(timestampOfRequest) < 0)
					try {
						requests.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
			//After it's notified and satisfies the requirements, it can enter the cs.	
			return;
		}else{
			synchronized(requests){
				requests.add(new Message(MessageType.CS_REQUEST_WRITE, null, timestampOfRequest));	//Add itself to the request queue
				writeRequests.add(new Message(MessageType.CS_REQUEST_WRITE, null, timestampOfRequest)); //Add itself to the write request queue
				while(requests.peek().clk.pid != pid)
					try {
						requests.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
			return;         //After it's notified and satisfies the requirements, it can enter the cs.
		}

	}
	
	/**
	 * Release the critial section, so that other server processes can enter the critial section.
	 * @param read true if read, false if write
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void releaseCritialSection(boolean read) throws IOException{
		/*if(message.type == MessageType.CS_REQUEST_READ) {
			readRequests.poll();
			for(ServerState serverstat : clusterInfo.values()){
				if(!serverstat.live || serverstat.pid == pid) continue;
				try {
					Socket socket = new Socket(serverstat.ipAddress, serverstat.port);
					sendMessage(socket, MessageType.CS_RELEASE, null);
					socket.close();
				} catch (UnknownHostException e) {
				} catch (IOException e) {
				}
			}
		}else {
			writeRequests.poll();
		}*/
		if(read) read_write_lock.release();
		else read_write_lock.release(MAX_READER_IN_A_SERVER);
	}
	
	/**
	 * Update the logical clock, increase the timestamp of this process by 1.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(){
		Clock ret = null;
		synchronized(clock_lock){
			//Enter critical section.
			ret = clock = new Clock(clock.timestamp+1, clock.pid);
			//Release critical section.
		}
		return ret;
	}
	
	/**
	 * Update the logical clock of this process according to a recerived timestamp.
	 * @param timestamp The timestamp of a message.
	 * @return The up to date clock.
	 */
	private static Clock updateClock(Clock timestamp){
		Clock ret = null;
		synchronized(clock_lock){
			//Enter critical section.
			if(timestamp == null) ret = clock;
			else
				ret = clock = new Clock(Math.max(clock.timestamp, timestamp.timestamp)+1, clock.pid);
			//Release critical section.
		}
		return ret;
	}

	/**
	 * Send a message through a socket
	 * @param msg The message to send
	 * @throws IOException If some io errors occur
	 */
	private static void sendMessage(Socket socket, Message msg) throws IOException{
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
		Thread monitor = new Thread(){ // Create a new thread, wait some time and shut down the stream.
			@Override
			public void run() {
				try {
					Thread.sleep(waitTime);
					istream.close();
				} catch (InterruptedException e) {}
				catch(IOException e){}	
			}
		};
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
		case CS_REQUEST_READ: 
			synchronized(requests){
				requests.add(msg);
			}
			break;
		case CS_REQUEST_WRITE:
			synchronized(requests){
				requests.add(msg);
				writeRequests.add(msg);
			}
			break;
		case CS_RELEASE:
			synchronized(requests){
				if(requests.poll().type == MessageType.CS_REQUEST_WRITE)
					writeRequests.poll();
				requests.notifyAll();
			}
			break;
		default:
			break;
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
				sendMessage(socket, new Message(MessageType.CLOCK_MESSAGE, null, updateClock()));
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
