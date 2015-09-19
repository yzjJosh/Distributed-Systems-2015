package server;

import java.io.*;
import java.net.*;

/**
 * Server thread is the thread where server runs. It waits for requests from clients and handle it.
 *
 */
public class ServerThread extends Thread {
	
	private final ObjectInputStream istream; //The input stream
	private final ObjectOutputStream ostream; //The output stream
	private Message prevMessage = new Message(null, null, null);	//The previously received message
	private Object lock = new Object();	//The lock
	
	
	/**
	 * Initialize a sever thread with a socket.
	 * @param socket The socket.
	 * @throws IOException If cannot initialize this thread due to an io error
	 */
	public ServerThread(Socket socket) throws IOException{
		this.istream = new ObjectInputStream(socket.getInputStream());
		this.ostream = new ObjectOutputStream(socket.getOutputStream());
	}
	
	/**
	 * Initialize a server thread with a socket input and output stream.
	 * @param istream	//The input stream
	 * @param ostream	//The output stream
	 */
	public ServerThread(ObjectInputStream istream, ObjectOutputStream ostream){
		this.istream = istream;
		this.ostream = ostream;
	}
	
	@Override
	public void run(){
		System.out.println("Server thread "+Thread.currentThread().getId()+", starts.");
		try {
			while(true){			
				Message msg = (Message)istream.readObject();	//Listen to messages
				synchronized(lock){
					prevMessage = msg;			//Put new message
					lock.notifyAll();	//Notify all threads waiting for new message
				}
				Server.onReceivingMessage(prevMessage, ostream);	//Throw the new message to server for response
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {}
		System.err.println("Server thread "+Thread.currentThread().getId()+" terminates.");
	}
	
	/**
	 * Wait for the next message received. This method call is blocking. Warning: message returned by this
	 * method are also exposed to other threads. Be careful with cocurrent issues when dealing with the message.
	 * @return The next message received
	 * @throws InterruptedException If the waiting thread is interrupted.
	 */
	public Message waitForMessage() throws InterruptedException{
		Message ret = null;
		synchronized(lock){
			ret = prevMessage;
			try {
				lock.wait();
			} catch (InterruptedException e) {
				if(ret == prevMessage)
					throw e;
			}
			ret = prevMessage;
		}
		return ret;
	}
	
}
