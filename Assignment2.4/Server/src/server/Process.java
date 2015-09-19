package server;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;


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
	private Object lock = new Object();		//The lock to make sure cocurrent performance
	
	
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
		thread.start();
	}
	
	/**
	 * Associate a connected socket to this process.(Passive connection)
	 * @param socket A connected socket
	 * @throws IOException If the ip or port of this socket does not match this process, or the socket is closed.
	 */
	public void connect(Socket socket) throws IOException{
		InetSocketAddress addr = (InetSocketAddress)socket.getRemoteSocketAddress();
		if(!ip.equals(addr.getHostName()) || addr.getPort() != port)
			throw new IOException("Ip and port does not match: "+addr+", which should be "+ip+":"+port);
		if(socket.isClosed())
			throw new IOException("Socket is closed!");
		send = new ObjectOutputStream(socket.getOutputStream());
		receive = new ObjectInputStream(socket.getInputStream());
		thread = new ServerThread(receive, send);	//Create a server thread to listen to incoming messages.
		thread.start();
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
	 * Receive a message from this process. This method is blocking. If no message received after 5s, a SocketTimeoutException
	 * will be thrown.
	 * @return The received message
	 * @throws SocketTimeoutException If no message received on time
	 * @throws IOException When the process is not connected
	 */
	public synchronized Message receiveMessage() throws IOException{
		if(receive == null || thread == null)
			throw new IOException("Process is not connected!");
		Message ret = null;
		final Thread waitThread = Thread.currentThread();
		Thread monitor = new Thread(){
			@Override
			public void run(){
				try {
					Thread.sleep(5000);
					waitThread.interrupt();
				} catch (InterruptedException e) {}	
			}
		};
		monitor.start();
		try {
			ret = thread.waitForMessage();
		} catch (InterruptedException e) {
			throw new SocketTimeoutException();
		}
		monitor.interrupt();
		return ret;
	}
}
