package communication;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import exceptions.TerminateException;

/**
 * Communication manager manages all aspects of network communications
 * 
 * @author Josh
 *
 */
public class CommunicationManager {
	
	private TreeSet<Integer> idSet = new TreeSet<Integer>();
	private int nextId = 0;
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
		Semaphore syncLock = new Semaphore(0);
		final WaitReplyTask task = new WaitReplyTask(msg, filter, listener, null, syncLock);
		final Semaphore messageSent = new Semaphore(0);
		task.waitThread = new Thread(){
			@Override
			public void run(){
				try {
					Thread.sleep(timeout);
					messageSent.acquire();
					fconncetion.removeWaitReplyTask(task);
					listener.OnReceiveError(CommunicationManager.this, id);
					task.sync.release();
				} catch (InterruptedException e) {}
			}
		};
		task.waitThread.start();
		while(!messageSent.hasQueuedThreads() && task.waitThread.getState() != Thread.State.TIMED_WAITING);
		fconncetion.sendAndWaitReplyTask(task);
		messageSent.release();
		if(sync)
			try {
				syncLock.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}
	
	/**
	 * Close an established connection
	 * @param id the id of the connection
	 */
	public void closeConnection(int id){
		ConnectionThread connection = null;
		synchronized (connections) {
			connection = connections.get(id);
		}
		if(connection != null)
			connection.cancelConnection();
	}
	
	private int getConncetionId(){
		synchronized(idLock){
			if(idSet.isEmpty()){
				return nextId++;
			}else
				return idSet.pollFirst();
		}
	}
	
	
	private void releaseConnectionId(int id){
		synchronized(idLock){
			if(id == nextId-1){
				nextId --;
				while(!idSet.isEmpty() && idSet.last() == nextId-1)
					nextId = idSet.pollLast();
			}else
				idSet.add(id);
		}
	}
	
	
	private class WaitConnectionThread extends Thread{
		
		private final int port;
		private final OnConnectionListener listener;
		private final LinkedBlockingQueue<Runnable> listenerRunQueue = new LinkedBlockingQueue<Runnable>(1000);
		private final Runnable stop = new Runnable(){
			@Override
			public void run(){
				throw new TerminateException();
			}
		};
		
		public WaitConnectionThread(int port, OnConnectionListener connectionlistener){
			this.port = port;
			this.listener = connectionlistener;
			new Thread(){
				@Override
				public void run(){
					try {
						while(true){
							try {
								listenerRunQueue.take().run();
							} catch (InterruptedException e) {}
						}
					} catch (TerminateException e) {}
				}
			}.start();
		}
		
		@Override
		@SuppressWarnings("resource")
		public void run(){
			int id = -1;
			try {
				ServerSocket server = new ServerSocket(port);
				while (true) {
					final Socket socket = server.accept();
					id = getConncetionId();
					ConnectionThread connectionThread = new ConnectionThread(id, socket);
					synchronized (connections) {
						connections.put(id, connectionThread);
					}
					final int fid = id;
					if(listener != null)
						while(true)
							try {
								listenerRunQueue.put(new Runnable(){
									@Override
									public void run(){
										listener.OnConnected(CommunicationManager.this, fid);
									}
								});
								break;
							} catch (InterruptedException e) {}
					connectionThread.start();
				}
			} catch (IOException e) {
				e.printStackTrace();
				if(id > -1)
					releaseConnectionId(id);
				if(listener != null){
					while(true)
						try {
							listenerRunQueue.put(new Runnable(){
								@Override
								public void run(){
									listener.OnConnectFail(CommunicationManager.this);
								}
							});
							break;
						} catch (InterruptedException e1) {}	
				}
			}
			while(true)
				try {
					listenerRunQueue.put(stop);
					break;
				} catch (InterruptedException e) {}
		}
	}
	
	private class ConnectionThread extends Thread{
		
		private final int connectionId;
		private final ObjectOutputStream ostream;
		private final ObjectInputStream istream;
		private OnMessageReceivedListener msgListener;
		private final Object listenerLock = new Object();
		private final Set<WaitReplyTask> waitReplyTasks = new HashSet<WaitReplyTask>();
		private final Socket socket;
		private final LinkedBlockingQueue<Runnable> listenerRunQueue = new LinkedBlockingQueue<Runnable>(1000);
		private boolean exit = false;
		private final Runnable stop = new Runnable(){
			@Override
			public void run(){
				throw new TerminateException();
			}
		};
			
		public ConnectionThread(int id, Socket socket) throws IOException{
			this.socket = socket;
			this.connectionId = id;
			this.ostream = new ObjectOutputStream(socket.getOutputStream());
			this.istream = new ObjectInputStream(socket.getInputStream());
			new Thread(){
				@Override
				public void run(){
					try {
						while(true){
							try {
								listenerRunQueue.take().run();
							} catch (InterruptedException e) {}
						}
					} catch (TerminateException e) {}
					exit = true;
					while(ConnectionThread.this.getState() != Thread.State.TIMED_WAITING
							&& ConnectionThread.this.getState() != Thread.State.TERMINATED);
					ConnectionThread.this.interrupt();
				}
			}.start();
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
		
		public synchronized void cancelConnection(){
			try {
				socket.close();
				System.err.println("CommunicationManager: Conncetion "+connectionId+" is closed!");
			} catch (IOException e) {}
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
					final Message msg = (Message)istream.readObject();
					synchronized(listenerLock){
						if(msgListener != null){
							final OnMessageReceivedListener listener = msgListener;
							while(true)
								try {
									listenerRunQueue.put(new Runnable(){
										@Override
										public void run(){
											listener.OnMessageReceived(CommunicationManager.this, connectionId, msg);
										}
									});
									break;
								} catch (InterruptedException e) {}
						}
					}
					synchronized(waitReplyTasks){
						LinkedList<WaitReplyTask> remove = new LinkedList<WaitReplyTask>();
						for(final WaitReplyTask task : waitReplyTasks)
							if(task.filter.filter(msg)){
								task.waitThread.interrupt();
								remove.add(task);
								if(task.listener != null)
									while(true)
										try {
											listenerRunQueue.put(new Runnable(){
												@Override
												public void run(){
													task.listener.OnMessageReceived(CommunicationManager.this, connectionId, msg);
													task.sync.release();
												}
											});
											break;
										} catch (InterruptedException e) {}
								else
									task.sync.release();
							}
						for(WaitReplyTask task: remove)
							waitReplyTasks.remove(task);
					}
				}
			} catch (Exception e) {
				synchronized(listenerLock){
					if(msgListener != null){
						final OnMessageReceivedListener listener = msgListener;
						while(true)
							try {
								listenerRunQueue.put(new Runnable(){
									@Override
									public void run(){
										listener.OnReceiveError(CommunicationManager.this, connectionId);
									}
								});
								break;
							} catch (InterruptedException e1) {}
					}
				}
				synchronized(waitReplyTasks){
					for(final WaitReplyTask task : waitReplyTasks){
						task.waitThread.interrupt();
						if(task.listener != null)
							while(true)
								try {
									listenerRunQueue.put(new Runnable(){
										@Override
										public void run(){
											task.listener.OnReceiveError(CommunicationManager.this, connectionId);
											task.sync.release();
										}
									});
									break;
								} catch (InterruptedException e1) {}
						else
							task.sync.release();	
					}
					waitReplyTasks.clear();
				}
			}
			while(true)
				try {
					listenerRunQueue.put(stop);
					break;
				} catch (InterruptedException e1) {}
			while(!exit)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
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
		public Semaphore sync;
		
		public WaitReplyTask(Message msg, MessageFilter filter, OnMessageReceivedListener listener, Thread waitThread, Semaphore sync){
			this.msg = msg;
			this.filter = filter;
			this.listener = listener;
			this.waitThread = waitThread;
			this.sync = sync;
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
							manager.sendMessage(id, new Message().put("words", "I have received your messge! Yeah!"));
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
					return !msg.containsKey("words");
				}
				
			}, 10000, new OnMessageReceivedListener(){

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
