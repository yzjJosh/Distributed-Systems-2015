package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
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

	private static TreeSet<Message> requests = new TreeSet<Message>();		  //The queue of waiting requests
	private static TreeSet<Message> writeRequests = new TreeSet<Message>();	  		//The queue of waiting write requests
	private static HashMap<Integer, LinkedList<Message>> requestsMap = new HashMap<Integer, LinkedList<Message>>(); //From pid to a request
	private static TheaterService service;	//The theater service object

	private static final int MAX_RESPONSE_TIME = 5000;	//The maximum response time of this system.
	
	//Synchronization locks
	private static Object clock_lock = new Object();	//clock access mutex lock
	private static final int MAX_READER_IN_A_SERVER = 20;	//Maximum number of concurent readers in each server.
	private static Semaphore read_write_lock = new Semaphore(MAX_READER_IN_A_SERVER);	//The read-write lock
	private static Semaphore cs_lock = new Semaphore(1);	//Lock to ensure that only one thread can call requestCS
	
	/**
	 *A thread that is good for synchronization
	 */
	private static class mThread extends Thread{
		public Semaphore m = new Semaphore(0);
	}
	
	/**
	 * Initialize the server process with an info file.
	 * @param infoFile The file where ips and ports are defined.
	 * @throws IOException If there is an error when reading the file.
	 */

	private static void init(String path, int maxNumOfSeates) throws IOException, FileNotFoundException{
		service  = new TheaterService(maxNumOfSeates);
		
		//Read the cluster information from a file.
		int id = 0;

		String serverInfo;
		BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
		while ((serverInfo = reader.readLine()) != null) {
			//Split the serverInfo to get the host and port.
			String[] splits = serverInfo.split(" ");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);	
			clusterInfo.put(id, new Process(id, ip, port));
			id++;
		}
		reader.close();
		
		//Get my pid.
		ServerSocket serversocket = null;
		System.out.println("My IP adress is: "+InetAddress.getLocalHost().getHostAddress());
		for(Process process : clusterInfo.values())
			if(process.ip.equals(InetAddress.getLocalHost().getHostAddress()))
				
				try{
					serversocket = new ServerSocket(process.port);
					pid = process.pid;
					break;
				}catch(IOException e){}
		
		if(serversocket == null)
			throw new IOException("Unable to find available port!");
		clock = new Clock(0, pid);	//Then initialize my clock
		System.out.println("This server got pid "+pid);
		
		
		//Try to find out if some servers are dead, and synchronize seate information
		HashMap<Process, mThread> threads = new HashMap<Process, mThread>();
		for(Process process : clusterInfo.values()){
			if(process.pid == pid) continue;		
			final Process p = process;
			threads.put(p, new mThread(){	//Create new thread to wait for response
				@Override public void run(){
					try {
						p.connect();	//Try to connect to a server
						Message msg = p.waitMessage(new MessageFilter(){
							@Override
							public boolean filt(Message m) {
								return m.type == MessageType.SERVER_SYNC_RESPONSE && m.clk.pid == p.pid;
							}	
						}, MAX_RESPONSE_TIME);	//Wait for p's response
						assert(msg.type == MessageType.SERVER_SYNC_RESPONSE);
					} catch (IOException e) {
						onProcessDied(p);
					}
					m.release();
				}
			});
			threads.get(p).start();
		}
		
		//Start synchronization, send SERVER_SYNC_START message
		for(Entry<Process, mThread> entry : threads.entrySet()){
			while(entry.getKey().live && entry.getValue().getState() != Thread.State.WAITING); //Wait until thread starts waiting
			if(entry.getKey().live){
				entry.getKey().message_event_lock();
				try{
					entry.getKey().sendMessage(new Message(MessageType.SERVER_SYNC_START, null, updateClock()));
				}catch(IOException e){}
				entry.getKey().message_event_unlock();
			}
		}

		//Wait until all processes have responded
		for(mThread thread : threads.values())
			try {
				thread.m.acquire();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		
		//Choose one process to synchronize data
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			final Process p = process;
			mThread waitThread = new mThread(){
				@SuppressWarnings("unchecked")
				@Override
				public void run(){
					Message msg;
					try {
						msg = p.waitMessage(new MessageFilter(){
							@Override
							public boolean filt(Message m) {
								return m.type == MessageType.SERVER_SYNC_RESPONSE && m.clk.pid == p.pid;
							}	
						}, MAX_RESPONSE_TIME);
						assert(msg.type == MessageType.SERVER_SYNC_RESPONSE);
						assert(msg.content != null);
						HashMap<String, Serializable> data = (HashMap<String, Serializable>) msg.content;
						service = (TheaterService) data.get("service");
						requests = (TreeSet<Message>) data.get("requests");
						writeRequests = (TreeSet<Message>) data.get("writeRequests");
						requestsMap	= (HashMap<Integer, LinkedList<Message>>) data.get("requestsMap");
						assert(service != null);
						assert(requests != null);
						assert(writeRequests != null);
						assert(requestsMap != null);
					} catch (IOException e) {
						onProcessDied(p);
					}	
					m.release();
				}
			};
			waitThread.start();
			while(p.live && waitThread.getState() == Thread.State.WAITING); // Wait until waitThread starts waiting
			p.message_event_lock();
			try{
				p.sendMessage(new Message(MessageType.SERVER_SYNC_DATA, null, updateClock()));	//Request sync data.
			}catch(IOException e){}
			p.message_event_unlock();
			try {
				waitThread.m.acquire();	// Wait until thread ends
			} catch (InterruptedException e) {} 
			if(p.live)
				break;
		}
		
		//Broadcast a confirmation to all servers so that they know this server is ready
		broadCastMessage(MessageType.SERVER_SYNC_COMPLETE, null);
		System.out.println("Synchronization success!");			
		System.out.println("Cluster infomation:");
		for(Process p : clusterInfo.values())
			System.out.println(p+(p.pid==pid?" (Me)":""));
		
		
		//After successfully initialize clusterInfo...
		try {
			new ClockUpdateThread(5000).start();	//Start the clock update thread
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
			cs_lock.acquire();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	
		final MessageType type = read? MessageType.CS_REQUEST_READ : MessageType.CS_REQUEST_WRITE;		//The sending message type.
		final MessageType ackType = read? MessageType.ACKNOWLEDGE_READ : MessageType.ACKNOWLEDGE_WRITE;	//The receiving message type.
		
		for(Process process : clusterInfo.values())
			process.message_event_lock();
		
		//Send the requests to all other servers
		LinkedList<mThread> l = new LinkedList<mThread>();
		final Message msg = new Message(type, null, updateClock());	//The request message
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			final Process p = process;
			l.add(new mThread(){
				@Override
				public void run(){
					try {
						p.waitMessage(new MessageFilter(){
							@Override
							public boolean filt(Message m) {
								return m.type == ackType && m.compareTo(msg) > 0 && m.clk.pid == p.pid;
							}
						}, MAX_RESPONSE_TIME); //Wait for its ack reply for 5s.
					}catch (IOException e){
						onProcessDied(p);	//No response, make it is died.
					}
					m.release();		
				}
			});
			l.peekLast().start();
			while(p.live && l.peekLast().getState() != Thread.State.WAITING);	//Wait until the thread starts waiting
			try{
				process.sendMessage(msg);
			}catch(IOException e){}
		}
		for(Process process : clusterInfo.values())
			process.message_event_unlock();
		
		//Wait until all threads stopped.
		for(mThread thread : l)
			try {
				thread.m.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		//---------------------------------------------------------------------------------------------------------------
		//If enter this line, then congratulations! You have received acks from all lived servers
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
		cs_lock.release();
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
		broadCastMessage(MessageType.CS_RELEASE, write? service: null);
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
	 * When a process dies, call this method to clear the process.
	 * @param process The died process
	 */
	public static void onProcessDied(Process process){
		synchronized(process){
			process.live = false;
			synchronized(requests){
				LinkedList<Message> msgs = requestsMap.remove(process.pid);
				if(msgs != null){
					for(Message msg : msgs){
						requests.remove(msg);
						writeRequests.remove(msg);
					}
				}
				System.err.println("pid="+process.pid+", addr="+process.ip+":"+process.port+", is dead");
				requests.notifyAll();
			}
		}
	}
	
	
	/**
	 * This method is called whenever a message is received through the server port. The message could be a client request or messages from 
	 * other servers. These messages will be processed and responded here. (This method may be called by different threads
	 * simontaneously, so be careful with concurrency when implementing it)
	 * @param msg The message received.
	 * @param process The process where this message is from. If this message is from client, pid of process will be -1.
	 * @throws IOException If there is an error when transferring data from socket.
	 */
	public static void onReceivingMessage(Message msg, Process process) throws IOException{
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
				process.message_event_lock();
				process.sendMessage(new Message(MessageType.ACKNOWLEDGE_READ, null, updateClock()));
				process.message_event_unlock();
				break;
				
			case CS_REQUEST_WRITE:
				//When receive the write request, add the request to the queue and write queue, then send back an acknowledgement.
				synchronized(requests){
					requests.add(msg);
					writeRequests.add(msg);
					LinkedList<Message> list = requestsMap.get(msg.clk.pid);
					if(list == null) requestsMap.put(msg.clk.pid, list = new LinkedList<Message>());
					list.add(msg);
				}
				process.message_event_lock();
				process.sendMessage(new Message(MessageType.ACKNOWLEDGE_WRITE, null, updateClock()));
				process.message_event_unlock();
				break;
				
			case CS_RELEASE:
				synchronized(requests){
					LinkedList<Message> list = requestsMap.get(msg.clk.pid);
					if(list == null) break;
					Message del = list.pollFirst();
					if(del == null) break;
					requests.remove(del);
					if(del.type == MessageType.CS_REQUEST_WRITE){
						writeRequests.remove(del);
						service = (TheaterService) msg.content;
					}
					requests.notifyAll();
				}
				break;
				
			case RESERVE_SEAT:    //When receiving a reserve request, to execute the following service.
				//enter cs
				requestCriticalSection(false);
				System.out.println("Got client request to reserve seates!");
				String[] contents = ((String) msg.content).split(" ");

				try {
					//Reservation is successful
					Set<Integer> seats = service.reserve(contents[0], Integer.parseInt(contents[1]));
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Congratulations, " + contents[0] + "! You have successfully reserved Seat" + seats, null));
					process.message_event_unlock();
					System.out.println("Reservation Success!!");	
				} catch (NumberFormatException e) {
					
				} catch (NoEnoughSeatsException e) {
					//There is not enough seats
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry, " +contents[0] +  ". The seats is not enough for your reservation! \n", null));
					process.message_event_unlock();
					System.out.println("No enough seates found!");
				} catch (RepeateReservationException e) {
					//The reservation is repeated
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry, " +contents[0] +  ". You have reserved the seats: " + e.reservedSeats, null));
					process.message_event_unlock();
					System.out.println("Repeated reservation!");
				}
				//release cs
				releaseCriticalSection();
				break;
				
			case SEARCH_SEAT:
				//Enter cs as a reader
				requestCriticalSection(true);
				System.out.println("Received search request from client");
				try {
					Set <Integer> seats = service.search((String)msg.content);
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Hello! " +  "Mr/Ms " + (String)msg.content + "! Your reserved seats are " + seats.toString(), null));
					process.message_event_unlock();
					System.out.println("Search is successful!");
				} catch (NoReservationInfoException e) {
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information for Mr/Ms " + (String)msg.content +" has been found ", null));
					process.message_event_unlock();
					System.out.println("Unable to find data!");
				}
				//Leave cs
				releaseCriticalSection();
				break;
				
			case DELETE_SEAT:
				//Enter cs as  a writer
				requestCriticalSection(false);
				System.out.println("Got seate deletion request");
				try {
					//num = the number of the released seats
					int[] num = service.delete((String)msg.content);
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Success! Your reserved " + num[0] + "seats are released! " + "Number of Left seats is " + num[1] , null));
					process.message_event_unlock();
					System.out.println("Deletion is successful!");
				} catch (NoReservationInfoException e) {
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information has been found \n", null));
					process.message_event_unlock();
					System.out.println("Unable to find data!");
				}
				//Leave cs
				releaseCriticalSection();
				break;	
			
			case SERVER_SYNC_START:
				//Send back the seate information to the sync server.
				process.message_event_lock();
				process.sendMessage(new Message(MessageType.SERVER_SYNC_RESPONSE, null, updateClock()));
				process.message_event_unlock();
				break;
				
			case SERVER_SYNC_DATA:
				final Message message = msg;
				final Process proc = process;
				Thread waitThread = new Thread(){
					@Override
					public void run(){
						try{
							proc.waitMessage(new MessageFilter(){
								@Override
								public boolean filt(Message m) {
									return m.type == MessageType.SERVER_SYNC_COMPLETE && m.clk.pid == message.clk.pid;
								}
							}, MAX_RESPONSE_TIME);
						}catch(IOException e){}
						try {
							releaseCriticalSection();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				};
				requestCriticalSection(false);
				waitThread.start();
				while(waitThread.getState() != Thread.State.WAITING && waitThread.getState()!=Thread.State.TERMINATED);
				if(waitThread.getState() != Thread.State.WAITING) break;
				HashMap<String, Serializable> data = new HashMap<String, Serializable>();
				data.put("service", service);
				synchronized(requests){	
					data.put("requests",requests);
					data.put("writeRequests", writeRequests);
					data.put("requestsMap", requestsMap);
					process.message_event_lock();
					process.sendMessage(new Message(MessageType.SERVER_SYNC_RESPONSE, data, updateClock()));
					process.message_event_unlock();
				}
				break;

			case SERVER_SYNC_COMPLETE:
				Process p = clusterInfo.get(msg.clk.pid);
				assert(p!=null);
				synchronized(p){
					assert(!p.live);
					p.live = true;	//That server is ready, so add it to the system.
					p.associate((ServerThread)Thread.currentThread());
				}
				System.out.println("pid="+p.pid+", addr="+p.ip+":"+p.port+", added to this system");
				break;
			default:
				break;
		}
	}
	
	/**
	 * Send the timestamped message to all other servers.
	 * @param type The type of message
	 * @param content Content of message
	 */
	public static void broadCastMessage(MessageType type, Serializable content){
		for(Process process : clusterInfo.values()){
			if(!process.live || process.pid == pid) continue;
			process.message_event_lock();
			try {
				process.sendMessage(new Message(type, content, updateClock()));
			} catch (IOException e) {}
			process.message_event_unlock();
		}
	}
	
	/**
	 * Entrance of the server process.
	 * @param args args[0] is the file where the server addresses and port# are defined.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */

//	public static void main(String[] args){
//
//		final String file = "servers.txt";
//		new Thread(){
//			@Override
//			public void run(){
//				try {
//					init(file);
//				} catch (FileNotFoundException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}.start();
//		
//		try {
//			Thread.sleep(8000);
//		} catch (InterruptedException e2) {
//			e2.printStackTrace();
//		}
  

		

	public static void main(String[] args) throws FileNotFoundException, IOException{
		init(args[0], Integer.parseInt(args[1]));

	}
}
