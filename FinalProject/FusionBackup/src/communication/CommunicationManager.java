package communication;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * Communication manager manages all aspects of network communications
 * 
 * @author Josh
 *
 */
public class CommunicationManager {
	
	private TreeSet<Integer> idSet = new TreeSet<Integer>();
	private int maxId = -1;
	private Object idLock = new Object();
	private HashMap<Integer, ConnectionThread> connections = new HashMap<Integer, ConnectionThread>();
	
	
	/**
	 * Waiting for network connection at some port asynchroniously
	 * @param port the port number to wait for connection
	 * @param connectionlistener the listener which listens to the connection event
	 */
	public void waitForConnection(int port, OnConnectionListener connectionlistener){
		new WaitConnectionThread(port, connectionlistener).start();
	}
	
	/**
	 * Connect to a host
	 * @param ip the ip address
	 * @param port the port number
	 * @return the id of this connection
	 * @throws IOException if an error occurs when trying to connect
	 */
	public int connect(String ip, int port) throws IOException{
		Socket socket = new Socket(ip, port);
		int id = getConncetionId();
		ConnectionThread connectionThread = null;
		try{
			connectionThread = new ConnectionThread(id, socket);
		}catch(IOException e){
			releaseConnectionId(id);
			throw e;
		}
		synchronized (connections) {
			connections.put(id, connectionThread);
		}
		connectionThread.start();
		return id;
	}
	
	/**
	 * Set OnMessageReceiveListener for a connection
	 * @param id the id of a connection
	 * @param listener the listener to set
	 */
	public void setOnMessageReceivedListener(int id, OnMessageReceivedListener listener){
		ConnectionThread connection = null;
		synchronized (connections) {
			connection = connections.get(id);
		}
		if(connection != null)
			connection.setOnMessageReceivedListener(listener);
	}
	
	/**
	 * Send a message through a connection
	 * @param id the id of the connection
	 * @param msg the message to send
	 * @throws IOException there is an error when sending a message
	 */
	public void sendMessage(int id, Message msg) throws IOException{
		ConnectionThread connection = null;
		synchronized (connections) {
			connection = connections.get(id);
		}
		if(connection != null)
			connection.transmit(msg);
	}
	
	/**
	 * Send a message through a connection and expect a specific reply from it
	 * @param id the id of the connection 
	 * @param msg the message to send
	 * @param filter a filter to decide if a message is an acceptable reply
	 * @param timeout the maximum time to wait for the reply
	 * @param listener a listener which is called when the reply is received or time out
	 * @param sync if the waiting is synchronious (block current thread) or not (run on background)
	 * @throws IOException if there is an error when sending message
	 */
	public void sendMessageForResponse(final int id, final Message msg, final MessageFilter filter, final long timeout, final OnMessageReceivedListener listener, final boolean sync) throws IOException{
		if(filter == null || timeout < 0)
			throw new IllegalArgumentException();
		ConnectionThread connection = null;
		synchronized (connections) {
			connection = connections.get(id);
		}
		if(connection == null) return;
		final ConnectionThread fconncetion = connection;
		final WaitReplyTask task = new WaitReplyTask(msg, filter, listener, null);
		final Semaphore threadTerminal = new Semaphore(0);
		final Semaphore messageSent = new Semaphore(0);
		task.waitThread = new Thread(){
			@Override
			public void run(){
				try {
					Thread.sleep(timeout);
					messageSent.acquire();
					fconncetion.removeWaitReplyTask(task);
					listener.OnReceiveError(CommunicationManager.this, id);
				} catch (InterruptedException e) {}
				threadTerminal.release();
			}
		};
		task.waitThread.start();
		while(!messageSent.hasQueuedThreads() && task.waitThread.getState() != Thread.State.TIMED_WAITING);
		fconncetion.sendAndWaitReplyTask(task);
		messageSent.release();
		if(sync)
			try {
				threadTerminal.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}
	
	private int getConncetionId(){
		synchronized(idLock){
			if(idSet.isEmpty()){
				return ++maxId;
			}else
				return idSet.pollFirst();
		}
	}
	
	
	private void releaseConnectionId(int id){
		synchronized(idLock){
			if(id == maxId-1){
				maxId --;
				while(!idSet.isEmpty() && idSet.last() == maxId-1)
					maxId = idSet.pollLast();
			}else
				idSet.add(id);
		}
	}
	
	
	private class WaitConnectionThread extends Thread{
		
		private final int port;
		private final OnConnectionListener listener;
		
		public WaitConnectionThread(int port, OnConnectionListener connectionlistener){
			this.port = port;
			this.listener = connectionlistener;
		}
		
		@Override
		@SuppressWarnings("resource")
		public void run(){
			int id = -1;
			try {
				ServerSocket server = new ServerSocket(port);
				while (true) {
					Socket socket = server.accept();
					id = getConncetionId();
					ConnectionThread connectionThread = new ConnectionThread(id, socket);
					synchronized (connections) {
						connections.put(id, connectionThread);
					}
					if(listener != null)
						listener.OnConnected(CommunicationManager.this, id);
					connectionThread.start();
				}
			} catch (IOException e) {
				e.printStackTrace();
				if(id > -1)
					releaseConnectionId(id);
				if(listener != null)
					listener.OnConnectFail(CommunicationManager.this);
			}
		}
	}
	
	private class ConnectionThread extends Thread{
		
		private final int connectionId;
		private final ObjectOutputStream ostream;
		private final ObjectInputStream istream;
		private OnMessageReceivedListener msgListener;
		private final Object listenerLock = new Object();
		private final Set<WaitReplyTask> waitReplyTasks = new HashSet<WaitReplyTask>();
			
		public ConnectionThread(int id, Socket socket) throws IOException{
			this.connectionId = id;
			this.ostream = new ObjectOutputStream(socket.getOutputStream());
			this.istream = new ObjectInputStream(socket.getInputStream());
		}
		
		public void setOnMessageReceivedListener(OnMessageReceivedListener msgListener){
			synchronized(listenerLock){
				this.msgListener = msgListener;
			}
			this.interrupt();
		}
		
		public void sendAndWaitReplyTask(WaitReplyTask task) throws IOException{
			assert(task != null);
			assert(task.filter != null);
			assert(task.waitThread != null);
			synchronized(waitReplyTasks){
				transmit(task.msg);
				waitReplyTasks.add(task);
			}
			this.interrupt();
		}
		
		public void removeWaitReplyTask(WaitReplyTask task){
			assert(task != null);
			synchronized(waitReplyTasks){
				waitReplyTasks.remove(task);
			}
		}

		
		public synchronized void transmit(Message msg) throws IOException{
			ostream.reset();
			ostream.writeObject(msg);
		}
		
		@Override
		public void run(){
			try {
				while(true){
					try {
						while(msgListener == null && waitReplyTasks.isEmpty()){
							Thread.sleep(1000);
						}
					} catch (InterruptedException e) {}
					Message msg = (Message)istream.readObject();
					synchronized(listenerLock){
						if(msgListener != null)
							msgListener.OnMessageReceived(CommunicationManager.this, connectionId, msg);
					}
					synchronized(waitReplyTasks){
						LinkedList<WaitReplyTask> remove = new LinkedList<WaitReplyTask>();
						for(WaitReplyTask task : waitReplyTasks)
							if(task.filter.filter(msg)){
								task.waitThread.interrupt();
								if(task.listener != null)
									task.listener.OnMessageReceived(CommunicationManager.this, connectionId, msg);
								remove.add(task);
							}
						for(WaitReplyTask task: remove)
							waitReplyTasks.remove(task);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				synchronized(listenerLock){
					if(msgListener != null)
						msgListener.OnReceiveError(CommunicationManager.this, connectionId);
				}
				synchronized(waitReplyTasks){
					for(WaitReplyTask task : waitReplyTasks){
						task.waitThread.interrupt();
						if(task.listener != null)
							task.listener.OnReceiveError(CommunicationManager.this, connectionId);
					}
					waitReplyTasks.clear();
				}
			}
			synchronized (connections) {
				connections.remove(connectionId);
			}
			releaseConnectionId(connectionId);
		}
		
	}
	
	private class WaitReplyTask{
		public Message msg;
		public MessageFilter filter;
		public OnMessageReceivedListener listener;
		public Thread waitThread;
		
		public WaitReplyTask(Message msg, MessageFilter filter, OnMessageReceivedListener listener, Thread waitThread){
			this.msg = msg;
			this.filter = filter;
			this.listener = listener;
			this.waitThread = waitThread;
		}
	}
	
	
	/**
	 * Test
	 * @param args
	 */
	public static void main(String[] args){
		CommunicationManager c = new CommunicationManager();
		CommunicationManager s = new CommunicationManager();
		s.waitForConnection(12345, new OnConnectionListener(){

			@Override
			public void OnConnected(CommunicationManager manager, int id) {
				manager.setOnMessageReceivedListener(id, new OnMessageReceivedListener(){

					@Override
					public void OnMessageReceived(CommunicationManager manager,
							int id, Message msg) {
						System.out.println("Server received message: "+msg);
						try {
							manager.sendMessage(id, msg.put("words", "I have received your messge! Yeah!"));
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void OnReceiveError(CommunicationManager manager,
							int id) {
						System.err.println("Receive fails!");
					}
					
				});
				
			}

			@Override
			public void OnConnectFail(CommunicationManager manager) {
				System.err.println("Connection fails!");
			}
			
		});
		try {
			int id = c.connect(Inet4Address.getLocalHost().getHostAddress(), 12345);
			c.sendMessageForResponse(id, new Message().put("I am client", "Josh"), new MessageFilter(){

				@Override
				public boolean filter(Message msg) {
					return msg.containsKey("words");
				}
				
			}, 5000, new OnMessageReceivedListener(){

				@Override
				public void OnMessageReceived(CommunicationManager manager,
						int id, Message msg) {
					System.out.println("Client receive message "+msg);
				}

				@Override
				public void OnReceiveError(CommunicationManager manager, int id) {
					System.err.println("Receive fails!");		
				}
				
			}, true);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		System.out.println("Execution finish!");
	}
	
}
