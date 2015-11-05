package client;
import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;

import message.Message;


/**
 * Process is a process of distributed system.
 *
 */
public class ProcessForClient {
	public final int pid;					//The pid of a server.
	public final String ip;					//The ip of a server
	public final int port;					//The port of a server.
	private ObjectOutputStream send;		//The stream to send data to the server
	private ObjectInputStream receive;		//The stream to receive data from server
	public boolean live;					//If the server process live or dead.
	private Object lock = new Object();
	
	/**
	 * Create a new process object
	 * @param pid	The pid of the process
	 * @param ip	The ip of the process
	 * @param port	The port of the process
	 * @param live	If it is live or dead
	 */
	public ProcessForClient(int pid, String ip, int port, boolean live){
		
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
	public ProcessForClient(int pid, String ip, int port){
		this(pid, ip, port, true);
	}
	
	
	/**
	 * Try to connect to that process
	 * @throws IOException If there is an error occurs
	 */
	public void connect() throws IOException{
		synchronized(lock){
			Socket socket = new Socket(ip, port);
		  //  socket.setSoTimeout(10 * 1000);
			send = new ObjectOutputStream(socket.getOutputStream());
			receive = new ObjectInputStream(socket.getInputStream());
		}
	}
	
	/**
	 * Send a message to this process
	 * @param msg The message
	 * @throws IOException If there is an error occurs
	 */
	public void sendMessage(Message msg) throws IOException{
		if(send == null)
			throw new IOException("Process is not connected!");
		synchronized(lock){
			send.writeObject(msg);
		}
	}
	
	/**
	 * Receive a message from this process. This method is blocking. If no message received after 5s, a SocketTimeoutException
	 * will be thrown.
	 * @return The received message
	 * @throws IOException If there is an io error occurs
	 */
	public Message receiveMessage() throws IOException{
		if(receive == null)
			throw new IOException("Process is not connected! Wait for re-connection!");
		Message ret = null;
		
		try {
			ret = (Message)receive.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
//		
//		synchronized(lock){
//			try {
//				ret = (Message)receive.readObject();
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//		}
		return ret;
	}
	
	@Override
	public String toString(){
		return "Process "+pid+": addr="+ip+":"+port+", live="+live;
	}
	
}
