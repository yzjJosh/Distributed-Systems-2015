package chord;
import java.io.*;
import java.net.*;

public class ChordNodeThread extends Thread {
	final ObjectInputStream istream; //The input stream
	final ObjectOutputStream ostream; //The output stream
	private Socket socket;
	/**
	 * Initialize a chord node thread with a socket input and output stream.
	 * @param istream	//The input stream
	 * @param ostream	//The output stream
	 */
	public ChordNodeThread(ObjectInputStream istream, ObjectOutputStream ostream) {
		this.istream = istream;
		this.ostream = ostream;
	}

	/**
	 * Initialize a sever thread with a socket.
	 * @param socket The socket.
	 * @throws IOException If cannot initialize this thread due to an io error
	 */
	public ChordNodeThread(Socket socket) throws IOException{
		this(new ObjectInputStream(socket.getInputStream()), new ObjectOutputStream(socket.getOutputStream()));
	}
	
	@Override
	public void run() {
		
	}
}
