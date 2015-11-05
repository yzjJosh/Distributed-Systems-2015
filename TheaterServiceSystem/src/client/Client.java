package client;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JButton;
import javax.swing.JScrollPane;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JLabel;

import message.Message;

import javax.swing.JTextArea;

import java.awt.Color;
import java.awt.Font;
import javax.swing.ImageIcon;
import java.awt.SystemColor;
public class Client extends JFrame {

	private static final long serialVersionUID = 1L;
	
	ObjectOutputStream writer = null;
	ObjectInputStream reader = null;  //A buffer to store the message from the server
	private static final HashMap<Integer, ProcessForClient> clusterInfo = new HashMap<Integer, ProcessForClient>(); //Pid to every srever's state in the cluster.
	private File file = null;
	private ProcessForClient server = null;
	JTextArea messageArea = new JTextArea();
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
				ProcessForClient process = new ProcessForClient(i, ip, port, true);
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
		getContentPane().setForeground(Color.GRAY);
		setBackground(Color.LIGHT_GRAY);
		getContentPane().setBackground(Color.LIGHT_GRAY);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setVisible(true);
		setBounds(100, 100, 568, 318);
		getContentPane().setLayout(null);
		
		JButton btnReservation = new JButton("Reservation");
		btnReservation.setFont(new Font("Lao MN", Font.BOLD | Font.ITALIC, 13));
		btnReservation.setForeground(Color.BLACK);
		btnReservation.setBackground(Color.WHITE);
		btnReservation.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
		        	Reserve re = new Reserve(server);
				    re.setVisible(true);				        	
				
			
			}
		});
		
		btnReservation.setBounds(66, 59, 117, 73);
		getContentPane().add(btnReservation);
		
		JButton btnSearch = new JButton("Search");
		btnSearch.setFont(new Font("Lao MN", Font.BOLD | Font.ITALIC, 13));
		btnSearch.setBackground(Color.WHITE);
		btnSearch.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				Search se = new Search(server);
				se.setVisible(true);
			}
		});
		btnSearch.setBounds(66, 128, 117, 73);
		getContentPane().add(btnSearch);
		
		JButton btnDelete = new JButton("Delete");
		btnDelete.setFont(new Font("Lao MN", Font.BOLD | Font.ITALIC, 13));
		btnDelete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Delete de = new Delete(server);
				de.setVisible(true);
			}
		});
		btnDelete.setBounds(66, 199, 117, 73);
		getContentPane().add(btnDelete);
		
		JLabel lblWelcomeToUse = new JLabel("Welcome to use the client reservation system!");
		lblWelcomeToUse.setForeground(Color.WHITE);
		lblWelcomeToUse.setFont(new Font("Avenir Next", Font.BOLD, 15));
		lblWelcomeToUse.setBounds(78, 22, 414, 25);
		getContentPane().add(lblWelcomeToUse);
		
		JLabel lblFromServerSystem = new JLabel("From server system:");
		lblFromServerSystem.setForeground(Color.WHITE);
		lblFromServerSystem.setFont(new Font("Helvetica", Font.BOLD, 13));
		lblFromServerSystem.setBounds(231, 66, 147, 16);
		getContentPane().add(lblFromServerSystem);
		messageArea.setBackground(SystemColor.textHighlight);
		
		
		messageArea.setBounds(26, 26, 295, 162);
		getContentPane().add(messageArea);
		messageArea.setLineWrap(true);
		
		JScrollPane scroll=new JScrollPane(messageArea);
		scroll.setBounds(231,94,295,162);
		getContentPane().add(scroll);
		
		
		JLabel lblNewLabel = new JLabel("New label");
		lblNewLabel.setIcon(new ImageIcon("Stars.jpg"));
		lblNewLabel.setBounds(0, 0, 568, 290);
		getContentPane().add(lblNewLabel);
		
	}
	/**
	 * Randomly choose a server to connect
	 */
	public void connectToServer() {
		for(ProcessForClient p : clusterInfo.values())
			p.live = true;
		int dieNum = 0;
		while(true){
			try {
				//Initialize the socket
				//Choose a random live server to connect.
				do{
					int chosenServer = new Random().nextInt(clusterInfo.size());
					server = clusterInfo.get(chosenServer);
				}while(!server.live);
				System.out.println("Try to connect to " + server);
				server.connect();
				System.out.println("Connect success!!!!!!");
				server.live = true;
				break;
			} catch(IOException e) {
				//The connection failed, make the live tag false and reconnect
				System.out.println("Connect Failed!  ");
				server.live = false;
				dieNum++;
			}
			if(dieNum == clusterInfo.size()){
				dieNum = 0;
				for(ProcessForClient p : clusterInfo.values())
					p.live = true;
			}
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
			reply =  server.receiveMessage();
		} catch (IOException e) {
			connectToServer();
		}	
		//Message from the server is not null, show the message.
		if(reply != null) {
			String content = (String) reply.content;
			messageArea.setText(content);			
		}
	}
	
	public static void main(String[] args) throws Exception{
		final Client client = new Client();
		String path = args[0];
		client.readServerInfo(path);

		System.out.println(client.clusterInfo);
		new Thread() {
			@Override 
			public void run(){
				while(true){
					client.connectToServer();
					while(true){
						Message reply = null;
						try {
							reply =  client.server.receiveMessage();
							client.messageArea.append((String)reply.content + '\n');
						} catch (IOException e) {
							break;
						}
						//System.out.println((String)reply.content);
					}

				}
			}
		}.start();
      
//			client.refreshMessage();

		
	}
}
