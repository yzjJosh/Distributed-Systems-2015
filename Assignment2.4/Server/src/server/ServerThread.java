package server;

import java.io.*;
import java.net.Socket;

import message.Message;

/**
 * Server thread is the thread where server runs. It waits for requests from clients and handle it.
 *
 */
public class ServerThread extends Thread {
	
	private final Socket socket; // The socket
	
	/**
	 * Initialize a sever thread with a socket.
	 * @param socket The socket.
	 */
	public ServerThread(Socket socket){
		this.socket = socket;
	}
	
	@Override
	public void run(){
		
		try {
			ObjectInputStream istream = new ObjectInputStream(socket.getInputStream());
			ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
			while(true)
				Server.onReceivingMessage((Message)istream.readObject(), ostream);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Server thread "+Thread.currentThread().getId()+" terminates.");
		}
	}
	
}
