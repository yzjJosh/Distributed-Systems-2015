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
	private static final TreeSet<Message> requests = new TreeSet<Message>();		  //The queue of waiting requests
	private static final TreeSet<Message> writeRequests = new TreeSet<Message>();	  		//The queue of waiting write requests
	private static final HashMap<Integer, LinkedList<Message>> requestsMap = new HashMap<Integer, LinkedList<Message>>(); //From pid to a request
	private static TheaterService service = new TheaterService(20);	//The theater service object
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

	private static void init(String path) throws IOException, FileNotFoundException{
		
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
		System.out.println(InetAddress.getLocalHost().getHostAddress());
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

						//TO-DO synchronize seate information here using the msg.content

					} catch (IOException e) {
						synchronized(p){
							p.live = false;
						}
					}
					m.release();
				}
			});
			threads.get(p).start();
		}
		
		for(Entry<Process, mThread> entry : threads.entrySet()){
			while(entry.getKey().live && entry.getValue().getState() != Thread.State.WAITING); //Wait until thread starts waiting
			if(entry.getKey().live){
				entry.getKey().message_event_lock();
				entry.getKey().sendMessage(new Message(MessageType.SERVER_SYNC, null, updateClock()));
				entry.getKey().message_event_unlock();
			}
		}

		//Wait until all threads have stopped
		for(mThread thread : threads.values())
			try {
				thread.m.acquire();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
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
						synchronized(p){
							p.live = false;	//If no response, set it dead.
						}
						System.err.println("pid="+p.pid+", addr="+p.ip+":"+p.port+", is dead");
					}
					m.release();		
				}
			});
			l.peekLast().start();
			while(p.live && l.peekLast().getState() != Thread.State.WAITING);	//Wait until the thread starts waiting
			try{
				process.sendMessage(msg);
			}catch(IOException e){
				synchronized(process){
					process.live = false;
				}
			}
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
		broadCastMessage(MessageType.CS_RELEASE, null);
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
				for(String s : contents){
					System.out.println(s);	
				}
				try {
					//Reservation is successful
					Set<Integer> seats = service.reserve(contents[0], Integer.parseInt(contents[1]));
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "You have successfully reserved Seat" + (Serializable)seats, null));
					process.message_event_unlock();
					System.out.println("Reservation Success!!");	
				} catch (NumberFormatException e) {
					
				} catch (NoEnoughSeatsException e) {
					//There is not enough seats
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! The seats is not enough for your reservation! \n", null));
					process.message_event_unlock();
				} catch (RepeateReservationException e) {
					//The reservation is repeated
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! You have reserved the seats! \n", null));
					process.message_event_unlock();
				}
				//release cs
				releaseCriticalSection();
				break;
				
			case SEARCH_SEAT:
				//Enter cs as a reader
				requestCriticalSection(true);
				try {
					Set <Integer> seats = service.search((String)msg.content);
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Congratulations! Your reserved seats are " + seats.toString() + "\n", null));
					process.message_event_unlock();
				} catch (NoReservationInfoException e) {
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information has been found", null));
					process.message_event_unlock();
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
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Success! Your reserved " + num + "seats are released! \n", null));
					process.message_event_unlock();
				} catch (NoReservationInfoException e) {
					process.message_event_lock();
					updateClock();
					process.sendMessage(new Message(MessageType.RESPOND_TO_CLIENT, "Sorry! No reservation information has been found", null));
					process.message_event_unlock();
				}
				//Leave cs
				releaseCriticalSection();
				break;	
			
			case SERVER_SYNC:
				//Send back the seate information to the sync server.
				process.message_event_lock();
				process.sendMessage(new Message(MessageType.SERVER_SYNC_RESPONSE, null, updateClock()));
				process.message_event_unlock();
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
	public static void main(String[] args){

		final String file = "servers.txt";
		new Thread(){
			@Override
			public void run(){
				try {
					init(file);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}

	
//		pid = 1;
//		try {
//			System.out.println("Test ends!");
//			Server.init("/Users/mackbook/Distributed_System/Distributed-Systems-2015/Assignment2.4/Server/servers.txt");
//		} catch (IOException e2) {
//           System.out.println("ss");
//		}
//		System.out.println("Test ends!");

//		try {
//			ServerSocket seversocket = new ServerSocket(45678 + pid);
//			seversocket.accept();
//			System.out.println("let's start!");
//			seversocket.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}	

	/*	for(int j=0; j<3; j++){
			new Thread(){
				@Override
				public void run(){
					for(int i=0; i<50; i++){
						try {		
							requestCriticalSection(true);
						//	assert(requests.first().clk.pid == pid): "Pid="+requests.first().clk.pid+", which should be "+pid;
							//	System.out.println("Process "+pid+" enters CS"+" at "+System.currentTimeMillis()%10000+"<<<<<<<<<");
							//	Thread.sleep(1);
						//	System.out.println(pid+"-"+Thread.currentThread().getId()+"enters read cs.");
							File testFile = new File("E:\\USA\\courses\\Distributed_System\\Distributed-Systems-2015\\Assignment2.4\\test\\test.txt");
							BufferedReader br = new BufferedReader(new FileReader(testFile));
							int read = Integer.parseInt(br.readLine());
							br.close();
							br = new BufferedReader(new FileReader(testFile));
							int read2 = Integer.parseInt(br.readLine());
							br.close();
							br = new BufferedReader(new FileReader(testFile));
							int read3 = Integer.parseInt(br.readLine());
							assert(read == read2 && read2==read3);
							br.close();
						//	System.out.println(pid+"-"+Thread.currentThread().getId()+"releases read cs.");
							releaseCriticalSection();
							
							requestCriticalSection(false);
						//	System.out.println(pid+"-"+Thread.currentThread().getId()+"enters write cs.");
							testFile = new File("E:\\USA\\courses\\Distributed_System\\Distributed-Systems-2015\\Assignment2.4\\test\\test.txt");
							br = new BufferedReader(new FileReader(testFile));
							read = Integer.parseInt(br.readLine());
							br.close();
							BufferedWriter writer = new BufferedWriter(new FileWriter(testFile));
							writer.write(""+(read+1));
							writer.close();
						//	System.out.println(pid+"-"+Thread.currentThread().getId()+"releases write cs.");
							//	System.out.println("Process "+pid+" leave CS"+" at "+System.currentTimeMillis()%10000);
					//		assert(requests.first().clk.pid == pid): "Pid="+requests.first().clk.pid+", which should be "+pid+". clock="+clock;
							releaseCriticalSection();	
					//		assert(requests.isEmpty() || requests.first().clk.pid!=pid);
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
			}.start();
		}*/
		
	}
}
