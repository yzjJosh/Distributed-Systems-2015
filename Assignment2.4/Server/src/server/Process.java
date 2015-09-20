package server;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

import message.*;


/**
 * Process is a process of distributed system.
 *
 */
public class Process {
	public final int pid;					//The pid of a server.
	public final String ip;					//The ip of a server
	public final int port;					//The port of a server.
	private ObjectOutputStream send;		//The stream to send data to the server
	private ObjectInputStream receive;		//The stream to receive data from server
	public boolean live;					//If the server process live or dead.
	private ServerThread thread;			//The thread which listens to incoming messages from this process
	private Semaphore message_lock = new Semaphore(1);	//A semaphore associate with this process used for message event
	
	/**
	 * Create a new process object
	 * @param pid	The pid of the process
	 * @param ip	The ip of the process
	 * @param port	The port of the process
	 * @param live	If it is live or dead
	 */
	public Process(int pid, String ip, int port, boolean live){
		
		this.pid = pid;
		this.ip = ip;
		this.port = port;
		this.live = live;
	}
	
	/**
	 * Create a new live process object
	 * @param pid	The pid of the process
	 * @param ip	The ip address of the process
	 * @param port	The port of the process
	 */
	public Process(int pid, String ip, int port){
		this(pid, ip, port, true);
	}
	
	
	/**
	 * Try to connect to that process (Initiative connection)
	 * @throws IOException If there is an error occurs
	 */
	@SuppressWarnings("resource")
	public void connect() throws IOException{
		Socket socket = new Socket(ip, port);
		send = new ObjectOutputStream(socket.getOutputStream());
		receive = new ObjectInputStream(socket.getInputStream());
		thread = new ServerThread(receive, send);	//Create a server thread to listen to incoming messages.
		thread.process = this;
		thread.start();
	}
	
	/**
	 * Associate a working serverThread to this process.
	 * @param thread A working server thread
	 */
	public void associate(ServerThread thread){
		assert(thread != null);
		send = thread.ostream;
		receive = thread.istream;
		this.thread = thread;
		thread.process = this;
	}
	
	/**
	 * Send a message to this process
	 * @param msg The message
	 * @throws IOException If there is an error occurs
	 */
	public synchronized void sendMessage(Message msg) throws IOException{
		if(send == null)
			throw new IOException("Process is not connected!");
		send.writeObject(msg);
		send.flush();
	}
	
	/**
	 * Wait for a specific knid of message from this process for certain number of time. This method is blocking. If no such message received on time, a SocketTimeoutException
	 * will be thrown.
	 * @param filter The filter to filt specified message
	 * @param time The waiting time in ms
	 * @return The received message
	 * @throws SocketTimeoutException If no message received on time
	 * @throws IOException When the process is not connected
	 */
	public Message waitMessage(final MessageFilter filter, final int time) throws IOException{
		if(receive == null || thread == null)
			throw new IOException("Process is not connected!");
		Message ret = null;
		final Thread waitThread = Thread.currentThread();
		Thread monitor = new Thread(){
			@Override
			public void run(){
				try {
					Thread.sleep(time);
					waitThread.interrupt();
				} catch (InterruptedException e) {}	
			}
		};
		monitor.start();
		try {
			ret = thread.waitForMessage(filter);
		} catch (InterruptedException e) {
			throw new SocketTimeoutException();
		}
		monitor.interrupt();
		return ret;
	}
	
	/**
	 * Acquire operation for inner semaphore
	 */
	public void message_event_lock(){
		try {
			message_lock.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Release operation for inner semaphore
	 */
	public void message_event_unlock(){
		message_lock.release();
	}
	
	@Override
	public String toString(){
		return "Process "+pid+": addr="+ip+":"+port+", live="+live+", threadId="+(thread==null?null:thread.getId());
	}
}
