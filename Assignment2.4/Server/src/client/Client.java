package client;
import java.io.*;
import java.net.*;

import javax.swing.JFrame;
import javax.swing.JButton;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JLabel;

import com.sun.xml.internal.ws.message.stream.OutboundStreamHeader;

import javax.swing.JTextField;

import server.ClockUpdateThread;
import server.Server;
import server.ServerThread;
public class Client extends JFrame {
	InetAddress host = null;
	int port = 0;
	Socket client = null;
	Writer writer = null;
	private JTextField textField;
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
			}
		});
		btnSearch.setBounds(66, 128, 117, 65);
		getContentPane().add(btnSearch);
		
		JButton btnDelete = new JButton("Delete");
		btnDelete.setBounds(66, 205, 117, 57);
		getContentPane().add(btnDelete);
		
		JLabel lblWelcomeToUse = new JLabel("Welcome to use the client reservation system!");
		lblWelcomeToUse.setBounds(78, 31, 312, 16);
		getContentPane().add(lblWelcomeToUse);
		
		textField = new JTextField();
		textField.setBounds(243, 113, 134, 149);
		getContentPane().add(textField);
		textField.setColumns(10);
		
		JLabel lblFromServerSystem = new JLabel("From server system:");
		lblFromServerSystem.setBounds(243, 78, 147, 16);
		getContentPane().add(lblFromServerSystem);
		//Initialize the socket
		host = InetAddress.getLocalHost(); //Get local host IP
		port = 3333;
		client = new Socket(host, port);
		writer = new OutputStreamWriter(client.getOutputStream());
	}
	
	public static void main(String[] args) throws Exception{
		Client client = new Client();
		
	}

}
