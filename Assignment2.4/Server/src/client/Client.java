package client;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JButton;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JLabel;

import com.sun.xml.internal.ws.message.stream.OutboundStreamHeader;

import javax.swing.JTextField;

import message.Message;
import message.MessageFilter;
import message.MessageType;
import server.ClockUpdateThread;
import server.Server;
import server.Process;
import server.ServerThread;
public class Client extends JFrame {
	ObjectOutputStream writer = null;
	ObjectInputStream reader = null;  //A buffer to store the message from the server
	private static final HashMap<Integer, Process> clusterInfo = new HashMap<Integer, Process>(); //Pid to every srever's state in the cluster.
	private JTextField messageField;
	private File file = null;
	private Process server = null;
	/**
	 * Read the server information from the specified file
	 * @param path the file path
	 */
	public void readServerInfo(String path) {
		file = new File(path);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String serverInfo;
			int i = 0;
			while ((serverInfo = reader.readLine()) != null) {
				//Split the serverInfo to get the host and port.
				String[] splits = serverInfo.split(" ");
				String ip = splits[0];
				int port = Integer.parseInt(splits[1]);
				Process process = new Process(i, ip, port, false);
				clusterInfo.put(i, process);
				i++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try { //Close the reader.
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}
	/**
	 * Constructor
	 * @throws Exception
	 */
	public Client() throws Exception {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setVisible(true);
		setBounds(100, 100, 450, 300);
		getContentPane().setLayout(null);
		
		JButton btnReservation = new JButton("Reservation");
		btnReservation.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Reserve re = new Reserve(server);
				re.setVisible(true);
			}
		});
		btnReservation.setBounds(66, 59, 117, 57);
		getContentPane().add(btnReservation);
		
		JButton btnSearch = new JButton("Search");
		btnSearch.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				Search se = new Search(server);
			}
		});
		btnSearch.setBounds(66, 128, 117, 65);
		getContentPane().add(btnSearch);
		
		JButton btnDelete = new JButton("Delete");
		btnDelete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Delete de = new Delete(server);
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
		
	}
	/**
	 * Randomly choose a server to connect
	 */
	public void connectToServer() {
		try {
			//Initialize the socket
			//Choose a random live server to connect.
		int chosenServer = new Random().nextInt(clusterInfo.size());
		server = clusterInfo.get(chosenServer);
		server.connect();
		server.live = true;
		} catch(IOException e) {
			//The connection failed, make the live tag false and reconnect
			server.live = false;
			connectToServer();
		}
	}
	/**
	 * Refresh the message field to show the new message from the server
	 * @throws IOException when failed to read the message
	 * @throws ClassNotFoundException 
	 */
	public void refreshMessage() {
		Message reply = null;
		try {
			reply =  server.waitMessage(new MessageFilter(){

				@Override
				public boolean filt(Message msg) {
					return msg.type == MessageType.RESPOND_TO_CLIENT;
				}
				
			}, 5000);
		} catch (IOException e) {
			connectToServer();
		}	
		//Message from the server is not null, show the message.
		if(reply != null) {
			String content = (String) reply.content;
			messageField.setText(content);			
		}
	}
	public static void main(String[] args) throws Exception{
		Client client = new Client();
		
		String path = "servers.txt";
		client.readServerInfo(path);
		System.out.println(client.clusterInfo);
		
//		client.connectToServer();
//		while(true) {
//			client.refreshMessage();
//		}
		
	}

}
