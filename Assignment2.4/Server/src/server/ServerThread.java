package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import message.*;

/**
 * Server thread is the thread where server runs. It waits for requests from clients and handle it.
 *
 */
public class ServerThread extends Thread {
	
	final ObjectInputStream istream; //The input stream
	final ObjectOutputStream ostream; //The output stream
	Process process; //The process associated
	final private HashSet<LinkedBlockingQueue<Message>> waitingQueues = new HashSet<LinkedBlockingQueue<Message>>();
	
	/**
	 * Initialize a server thread with a socket input and output stream.
	 * @param istream	//The input stream
	 * @param ostream	//The output stream
	 */
	public ServerThread(ObjectInputStream istream, ObjectOutputStream ostream){
		this.istream = istream;
		this.ostream = ostream;
		new Process(-1,null,-1).associate(this);
	}
	
	/**
	 * Initialize a sever thread with a socket.
	 * @param socket The socket.
	 * @throws IOException If cannot initialize this thread due to an io error
	 */
	public ServerThread(Socket socket) throws IOException{
		this(new ObjectInputStream(socket.getInputStream()), new ObjectOutputStream(socket.getOutputStream()));
	}
	
	
	@Override
	public void run(){
		try {
			while(true){			
				Message msg = (Message)istream.readObject();	//Listen to messages
				Server.onReceivingMessage(msg, process);	//Throw the new message to server for response
				synchronized(waitingQueues){
					for(LinkedBlockingQueue<Message> q: waitingQueues)
						try {
							q.put(msg);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {}
	}
	
	/**
	 * Wait for the next message received. This method call is blocking. Warning: message returned by this
	 * method are also exposed to other threads. Be careful with cocurrent issues when dealing with the message.
	 * @return The next message received
	 * @throws InterruptedException If the waiting thread is interrupted.
	 */
	public Message waitForMessage(MessageFilter filter) throws InterruptedException{
		LinkedBlockingQueue<Message> mQueue = new LinkedBlockingQueue<Message>();
		synchronized(waitingQueues){
			waitingQueues.add(mQueue);
		}
		Message ret = null;
		while(!filter.filt(ret = mQueue.take()));
			
		synchronized(waitingQueues){
			waitingQueues.remove(mQueue);
		}
		return ret;
	}
	
}
