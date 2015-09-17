package client;
import java.io.*;
import java.net.*;
import java.util.HashMap;

import javax.swing.JFrame;
import javax.swing.JButton;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JLabel;

import com.sun.xml.internal.ws.message.stream.OutboundStreamHeader;

import javax.swing.JTextField;

import server.ClockUpdateThread;
import server.Server;
import server.ServerState;
import server.ServerThread;
public class Client extends JFrame {
	Socket client = null;
	ObjectOutputStream writer = null;
	ObjectInputStream reader = null;  //A buffer to store the message from the server
	private static final HashMap<Integer, ServerState> clusterInfo = new HashMap<Integer, ServerState>(); //Pid to every srever's state in the cluster.
	private JTextField messageField;
	private RandomAccessFile serversInfo = null;
	
	private String host = null;
	private int port = 0;
	/**
	 * Read a random server information from the specified file
	 * @param path
	 */
	public void ReadServerInfo(String path){
		try {
			serversInfo = new RandomAccessFile(path, "r");
			String server;
			try {
				server = serversInfo.readLine();
				String[] splits = server.split(" ");
			    host = splits[0];
			    port = Integer.parseInt(splits[1]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Constructor
	 * @throws Exception
	 */
	public Client() throws Exception {
		getContentPane().setLayout(null);
		JButton btnReservation = new JButton("Reservation");
		btnReservation.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Reserve re = new Reserve(writer);
			}
		});
		btnReservation.setBounds(66, 59, 117, 57);
		getContentPane().add(btnReservation);
		
		JButton btnSearch = new JButton("Search");
		btnSearch.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				Search se = new Search(writer);
			}
		});
		btnSearch.setBounds(66, 128, 117, 65);
		getContentPane().add(btnSearch);
		
		JButton btnDelete = new JButton("Delete");
		btnDelete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Delete de = new Delete(writer);
			}
		});
		btnDelete.setBounds(66, 205, 117, 57);
		getContentPane().add(btnDelete);
		
		JLabel lblWelcomeToUse = new JLabel("Welcome to use the client reservation system!");
		lblWelcomeToUse.setBounds(78, 31, 312, 16);
		getContentPane().add(lblWelcomeToUse);
		
		messageField = new JTextField();
		messageField.setBounds(243, 113, 134, 149);
		getContentPane().add(messageField);
		messageField.setColumns(10);
		
		JLabel lblFromServerSystem = new JLabel("From server system:");
		lblFromServerSystem.setBounds(243, 78, 147, 16);
		getContentPane().add(lblFromServerSystem);
		//Initialize the socket
		client = new Socket(host, port);
		writer = new ObjectOutputStream(client.getOutputStream());
		//Ready to read the server message
		reader = new ObjectInputStream(client.getInputStream());
		
		
	}
	/**
	 * Refresh the message field to show the new message from the server
	 * @throws IOException when failed to read the message
	 * @throws ClassNotFoundException 
	 */
	public void refreshMessage() throws IOException, ClassNotFoundException {
		client.setSoTimeout(5 * 1000); // set the timeout to 5s
	    String data = "";
	    try {
	    	data = (String) reader.readObject();
	    	messageField.setText(data);
	    } catch (SocketTimeoutException e) {
	    	messageField.setText("time is out! Connection to the server is off! \n");
	    	//Reconnect to a new server
	    	ReadServerInfo("ServersInfo.txt");
	    	client.connect(new InetSocketAddress(host, port));
	    }
	}
	public static void main(String[] args) throws Exception{
		Client client = new Client();
		client.ReadServerInfo("ServersInfo.txt");
		while(true) {
			client.refreshMessage();
		}
		
	}

}
