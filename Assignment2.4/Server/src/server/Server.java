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
	private static final TreeSet<Message> requests = new TreeSet<Message>();		  //The queue of waiting requests
	private static final TreeSet<Message> writeRequests = new TreeSet<Message>();	  		//The queue of waiting write requests
	private static final HashMap<Integer, LinkedList<Message>> requestsMap = new HashMap<Integer, LinkedList<Message>>(); //From pid to a request
	
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
	private static void init(String path) throws IOException {
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
		
		//After successfully initialize clusterInfo...
		ServerSocket serversocket = null;
		try {
			new ClockUpdateThread(5000).start();	//Start the clock update thread
			serversocket = new ServerSocket(clusterInfo.get(pid).port);	//Start the server socket, listening to new connections
			while(true){	//Keep doing
				Socket socket = serversocket.accept();	//Got a connection!
				new ServerThread(socket).start();	//Create a new server thread to serve this client.
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			
		}finally{
			if(serversocket != null)
				serversocket.close();
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
		
		final Lock test = new Lock();
		
		//Send the requests to all other servers
		class mThread extends Thread{
			Semaphore m = new Semaphore(0);
		}
		LinkedList<mThread> l = new LinkedList<mThread>();
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			final Process p = process;
			//l.add(new mThread(){
			//	@Override
			//	public void run(){
					synchronized(p){
						if(p.live){
							try {
								updateClock();
								p.sendMessage(msg);	//Send request to a server
								Message reply = p.receiveMessage(); //Wait for its ack reply for 5s.
								updateClock(reply.clk);
								assert(reply.type == ackType);
								assert(reply.compareTo(msg) > 0);
								assert(reply.clk.pid == p.pid);
			//					synchronized(test){
								test.num++;		
				//				}
							}catch (IOException e){
								p.live = false;	//If no response, set it dead.
								System.out.println("pid="+p.pid+", addr="+p.ip+":"+p.port+", is dead");
								e.printStackTrace();
							}
						}
					}
		//			m.release();
		//		}
		//	});
		//	l.peekLast().start();
			
			
		}
		for(mThread thread : l)
			try {
				thread.m.acquire();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		//---------------------------------------------------------------------------------------------------------------
		//If enter this line, then congratulations! You have received acks from all lived servers
		//assert(test.num == 9): "num is "+test.num;
		synchronized(requests){
			requests.add(msg);	//Add itself to the request queue
			LinkedList<Message> list = requestsMap.get(pid);
			if(list == null) requestsMap.put(pid, list = new LinkedList<Message>());
			list.add(msg); //Add the message to the pid to message map.
			if(read){
				//If there is at least one write request whose timestamp is smaller, it has to wait
				while(!writeRequests.isEmpty() && writeRequests.first().compareTo(msg) < 0)
					try {
						requests.wait();
					} catch (InterruptedException e) {}
			}else{
				writeRequests.add(msg); //Add itself to the write request queue
				while(requests.first() != msg)
					try {
						requests.wait();
					} catch (InterruptedException e) {}
			}
		}
		//After it's notified and satisfies the requirements, it can enter the cs.
		//System.out.println("Allowed to enter the critical section ("+msg+")");
	}
	
	/**
	 * Release the critial section, so that other server processes can enter the critial section.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	private static void releaseCriticalSection() throws IOException{
		boolean write = false;
		synchronized(requests){
			//Remove its request from the queue firstly
			LinkedList<Message> list = requestsMap.get(pid);
			assert(list != null);
			Message msg = list.pollFirst();
			assert(msg.clk.pid == pid);
			requests.remove(msg);
			if(write = (msg.type == MessageType.CS_REQUEST_WRITE))
				writeRequests.remove(msg);
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
				synchronized(requests){
					requests.add(msg);
					LinkedList<Message> list = requestsMap.get(msg.clk.pid);
					if(list == null) requestsMap.put(msg.clk.pid, list = new LinkedList<Message>());
					list.add(msg);
				}
				link.writeObject(new Message(MessageType.ACKNOWLEDGE_READ, null, updateClock()));
				break;
			case CS_REQUEST_WRITE:
				//When receive the write request, add the request to the queue and write queue, then send back an acknowledgement.
				synchronized(requests){
					if(!requests.isEmpty() && requests.first().clk.pid == pid)
						assert(requests.first().compareTo(msg) < 0): requests.first()+" is larger than "+msg; 
					requests.add(msg);
					writeRequests.add(msg);
					LinkedList<Message> list = requestsMap.get(msg.clk.pid);
					if(list == null) requestsMap.put(msg.clk.pid, list = new LinkedList<Message>());
					list.add(msg);
				}
				link.writeObject(new Message(MessageType.ACKNOWLEDGE_WRITE, null, updateClock()));
				break;
			case CS_RELEASE:
				//When receive release request, remove the request from the queue
				synchronized(requests){
					LinkedList<Message> list = requestsMap.get(msg.clk.pid);
					assert(list != null);
					Message del = list.pollFirst();
					requests.remove(del);
					if(del.type == MessageType.CS_REQUEST_WRITE)
						writeRequests.remove(del);
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
		
		link.flush();
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
		//Following code is for unit test
		pid = Integer.parseInt(args[0]);
		clock = new Clock(0, pid);
		int n = Integer.parseInt(args[1]);
		for(int i=0; i<n; i++)
			clusterInfo.put(i, new Process(i, "192.168.1.120", 12345+i, true));
		try {
			ServerSocket seversocket = new ServerSocket(clusterInfo.get(pid).port);
			seversocket.accept();
			System.out.println("let's start!");
			seversocket.close();
			new Thread(){
				@Override
				public void run(){
					try {
						Server.init(null);
					} catch (IOException e) {
					}
				}
			}.start();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(Process p : clusterInfo.values())
			try{
				if(p.pid < pid)
				p.connect();
			}catch(Exception e){}

		for(int i=0; i<50; i++){
			try {		
					assert(requests.isEmpty() || requests.first().clk.pid!=pid);
					requestCriticalSection(false);
					assert(requests.first().clk.pid == pid): "Pid="+requests.first().clk.pid+", which should be "+pid;
					/*System.out.println("The first one in request queue is:");
					synchronized(requests){
						System.out.println(requests.peek());
					}*/
				//	System.out.println("Process "+pid+" enters CS"+" at "+System.currentTimeMillis()%10000+"<<<<<<<<<");
				//	Thread.sleep(1);
					File testFile = new File("E:\\USA\\courses\\Distributed System\\test\\test.txt");
					BufferedReader br = new BufferedReader(new FileReader(testFile));
					int read = Integer.parseInt(br.readLine());
					br.close();
				//	releaseCritialSection();
				//	requestCritialSection(false);
					BufferedWriter writer = new BufferedWriter(new FileWriter(testFile));
					writer.write(""+(read+1));
					writer.close();

				//	System.out.println("Process "+pid+" leave CS"+" at "+System.currentTimeMillis()%10000);
					assert(requests.first().clk.pid == pid): "Pid="+requests.first().clk.pid+", which should be "+pid+". clock="+clock;
					releaseCriticalSection();	
					assert(requests.isEmpty() || requests.first().clk.pid!=pid);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("The request queue is:");
				synchronized(requests){
					for(Message m : requests)
						System.out.println(m);
				}
			}
		}
		System.out.println("Test ends!");
		
	}
}
