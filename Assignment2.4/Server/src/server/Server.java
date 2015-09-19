package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Semaphore;

import exceptions.NoEnoughSeatsException;
import exceptions.NoReservationInfoException;
import exceptions.RepeateReservationException;
import message.*;

/**
 * A server process in a distributed system.
 */
public class Server {
	
	private static Clock clock; //The Lamport's logical clock.
	private static int pid;		//The pid of current process.
	private static final HashMap<Integer, Process> clusterInfo = new HashMap<Integer, Process>(); //Pid to every srever's process in the cluster.
	private static final PriorityQueue<Message> requests = new PriorityQueue<Message>();		  //The queue of waiting requests
	private static final PriorityQueue<Message> writeRequests = new PriorityQueue<Message>();	  		//The queue of waiting write requests
	
	//Synchronization locks
	private static Object clock_lock = new Object();	//clock access mutex lock
	private static final int MAX_READER_IN_A_SERVER = 20;	//Maximum number of concurent readers in each server.
	private static Semaphore read_write_lock = new Semaphore(MAX_READER_IN_A_SERVER);	//The read-write lock
	private static TheaterService service = new TheaterService(pid);
	private static File file = null;
	
	/**
	 * Initialize the server process with an info file.
	 * @param infoFile The file where ips and ports are defined.
	 * @throws IOException If there is an error when reading the file.
	 */
	private static void init(String path) {
		file = new File(path);
		BufferedReader reader = null;
		Process process = null;
		int i = 0;
	
			try {
				reader = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			String serverInfo;
			String[] splits = null;
			String ip = null;
			int port = 0;
			try {
				while ((serverInfo = reader.readLine()) != null) {
					//Split the serverInfo to get the host and port.
					splits = serverInfo.split(" ");
					ip = splits[0];
					port = Integer.parseInt(splits[1]);
					// Try to find out if the server is alive by sending an ack and
					// check if the sender server can receive a response in time.
					process = new Process(i, ip, port, true);
					try {
						process.connect();						
						clusterInfo.put(i, process);
					}catch (SocketTimeoutException e) {
						process = new Process(i, ip, port, false);
						clusterInfo.put(i, process);			
					}
				    i++;
				}
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
	
		}
	
	
	
	/**
	 * Request critial section access. If critial section is unavailable, block the thread until it becomes available.
	 * @param read true if read, false if write
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void requestCriticalSection(boolean read) throws IOException {
		//Acquire lock firstly
		try {
			if(read) read_write_lock.acquire();
			else read_write_lock.acquire(MAX_READER_IN_A_SERVER);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	
		final MessageType type = read? MessageType.CS_REQUEST_READ : MessageType.CS_REQUEST_WRITE;		//The sending message type.
		final MessageType ackType = read? MessageType.ACKNOWLEDGE_READ : MessageType.ACKNOWLEDGE_WRITE;	//The receiving message type.
		final Message msg = new Message(type, null, updateClock());	//The request message
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
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			final Process p = process;
			synchronized(ackLock){
				ackLock.num ++;	//Send a request, lock.num++.
			}
			new Thread(){
				@Override
				public void run(){
					try {
						updateClock();
						p.sendMessage(msg);	//Send request to a server
						Message reply = null;
						while((reply = p.receiveMessage()).type != ackType);	//Wait for its ack reply for 5s.
						updateClock(reply.clk);
					}catch (IOException e){
						p.live = false;	//If no response, set it dead.
						System.out.println("pid="+p.pid+", addr="+p.ip+":"+p.port+", is dead");
					}
					//After receive the ACK or set the server dead, lock.num--.
					synchronized(ackLock){
						ackLock.num--;
						mainThread.interrupt();
					}
				}
			}.start();
		}
		
		while(ackLock.num > 0)
			try {
				Thread.sleep(1000);	//If have not received enough ack, sleep.
			} catch (InterruptedException e) {}
		//---------------------------------------------------------------------------------------------------------------
		//If enter this line, then congratulations! You have received acks from all lived servers
		
		if(read){
			synchronized(requests){
				requests.add(msg);	//Add itself to the request queue
				//If there is at least one write request whose timestamp is smaller, it has to wait
				while(!writeRequests.isEmpty() && writeRequests.peek().compareTo(msg) < 0)
					try {
						requests.wait();
					} catch (InterruptedException e) {}
			}
			//After it's notified and satisfies the requirements, it can enter the cs.	
			return;
		}else{
			synchronized(requests){
				requests.add(msg);	//Add itself to the request queue
				writeRequests.add(msg); //Add itself to the write request queue
				while(requests.peek() != msg)
					try {
						requests.wait();
					} catch (InterruptedException e) {}
			}
			return;         //After it's notified and satisfies the requirements, it can enter the cs.
		}

	}
	
	/**
	 * Release the critial section, so that other server processes can enter the critial section.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void releaseCriticalSection() throws IOException{
		boolean write = false;
		synchronized(requests){
			//Remove its request from the queue firstly
			if(write = (requests.poll().type == MessageType.CS_REQUEST_WRITE))
				writeRequests.poll();
		}
		//Then tell every server that I want to release the critical section
		broadCastMessage(new Message(MessageType.CS_RELEASE, null, null), true);
		if(write) read_write_lock.release(MAX_READER_IN_A_SERVER);
		else read_write_lock.release();
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
	 * This method is called whenever a message is received through the server port. The message could be a client request or messages from 
	 * other servers. These messages will be processed and responded here. (This method may be called by different threads
	 * simontaneously, so be careful with concurrency when implementing it)
	 * @param msg The message received.
	 * @param link The outputstream where you can send a reply.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	public static void onReceivingMessage(Message msg, ObjectOutputStream link) throws IOException{
		updateClock(msg.clk); //Update the clock firstly.
		switch(msg.type) {      //Add the message into the corresponding queue.
		case CS_REQUEST_READ: 
			//When receive the read request, add the request to the queue, then send back an acknowledgement.
			synchronized(requests) {
				requests.add(msg);
			}
			link.writeObject(new Message(MessageType.ACKNOWLEDGE_READ, null, updateClock()));
			break;
		case CS_REQUEST_WRITE:
			//When receive the write request, add the request to the queue and write queue, then send back an acknowledgement.
			synchronized(requests) {
				requests.add(msg);
				writeRequests.add(msg);
			}
			link.writeObject(new Message(MessageType.ACKNOWLEDGE_WRITE, null, updateClock()));
			break;
		case CS_RELEASE:
			//When receive release request, remove the request from the queue
			synchronized(requests) {
				if(requests.poll().type == MessageType.CS_REQUEST_WRITE)
					writeRequests.poll();
				requests.notifyAll();
			}
			break;
		case RESERVE_SEAT:    //When receiving a reserve request, to execute the following service.
			//enter cs
			requestCriticalSection(false);
			String[] contents = ((String) msg.content).split(" ");
			try {
				//Reservation is successful
				Set<Integer> seats = service.reserve(contents[0], Integer.parseInt(contents[1]));
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT,  (Serializable) seats, null));
			} catch (NumberFormatException e) {
				
			} catch (NoEnoughSeatsException e) {
				//There is not enough seats
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! The seats is not enough for your reservation! \n", null));
			} catch (RepeateReservationException e) {
				//The reservation is repeated
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! The seats is not enough for your reservation! \n", null));
			}
			//release cs
			releaseCriticalSection();
			break;
		case SEARCH_SEAT:
			//Enter cs as a reader
			requestCriticalSection(true);
			try {
				Set <Integer> seats = service.search((String)msg.content);
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Congratulations! Your reserved seats are " + seats.toString() + "\n", null));
			} catch (NoReservationInfoException e) {
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information has been found", null));
			}
			//Leave cs
			releaseCriticalSection();
			break;
		case DELETE_SEAT:
			//Enter cs as  a writer
			requestCriticalSection(false);
			try {
				//num = the number of the released seats
				int num = service.delete((String)msg.content);
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Success! Your reserved " + num + "seats are released! \n", null));
			} catch (NoReservationInfoException e) {
				link.writeObject(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information has been found", null));
			}
			//Leave cs
			releaseCriticalSection();
			break;
			
			
		    default:
			break;
		}
		
		
	}
	
	/**
	 * Send the timestamped message to all other servers.
	 * @param msg The message to broadcast
	 * @param usingCurTimestamp If using current timestamp. If true, use real-time timestamp. If false, use the timestamp
	 * of msg.
	 */
	public static void broadCastMessage(Message msg, boolean usingCurTimestamp){
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			try {
				if(usingCurTimestamp)
					process.sendMessage(new Message(msg.type, msg.content, updateClock()));
				else{
					updateClock();
					process.sendMessage(msg);
				}
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
		/*try {
			Server.init(args[0]);
			new ClockUpdateThread(5000).start();
			ServerSocket serversocket = new ServerSocket(clusterInfo.get(pid).port);
			while(true)
				new ServerThread(serversocket.accept()).start();
			
		} catch (IOException e) {
			e.printStackTrace();
			
		}*/
		
		//Following code is for unit test
		pid = Integer.parseInt(args[0]);
		clock = new Clock(0, pid);
		init("servers.txt");
		clusterInfo.put(0, new Process(0, "192.168.1.120", 12345, true));
		clusterInfo.put(1, new Process(1, "192.168.1.120", 12346, true));
		clusterInfo.put(2, new Process(2, "192.168.1.120", 12347, true));
		clusterInfo.put(3, new Process(3, "192.168.1.120", 12348, true));
		try {
			final ServerSocket seversocket = new ServerSocket(clusterInfo.get(pid).port);
			Socket socket = seversocket.accept();
			System.out.println("let's start!");
			new Thread(){
				@Override
				public void run(){
					while(true)
						try {
							new ServerThread(seversocket.accept()).start();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
			}.start();
			socket.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(Process p : clusterInfo.values())
			try{
				p.connect();
			}catch(Exception e){}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(int i=0; i<1000; i++){
			try {			
					requestCriticalSection(false);
					File testFile = new File("E:\\USA\\courses\\Distributed System\\test\\test.txt");
					BufferedReader br = new BufferedReader(new FileReader(testFile));
					int read = Integer.parseInt(br.readLine());
					br.close();
				//	releaseCritialSection();
				//	requestCritialSection(false);
					BufferedWriter writer = new BufferedWriter(new FileWriter(testFile));
					writer.write(""+(read+1));
					writer.close();
					releaseCriticalSection();	
			} catch (IOException e) {
				e.printStackTrace();
			}
			

		}
		
	}
}
