package client;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JTextField;
import javax.swing.JLabel;

import message.Message;
import message.MessageType;
import server.Process;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.util.Map;

public class Reserve extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private JTextField nameField;
	private JTextField countField;
	/**
	 * Create the dialog.
	 */
	public Reserve(final ProcessForClient server) {
	
		setBounds(100, 100, 450, 300);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(null);
	
		nameField = new JTextField();
		nameField.setBounds(200, 66, 134, 28);
		contentPanel.add(nameField);
		nameField.setColumns(10);
		
		countField = new JTextField();
		countField.setBounds(200, 137, 134, 28);
		contentPanel.add(countField);
		countField.setColumns(10);
		
		JLabel lblYourName = new JLabel("Your name:");
		lblYourName.setBounds(95, 72, 93, 16);
		contentPanel.add(lblYourName);
		
		JLabel lblReservationCount = new JLabel("Reservation Count:");
		lblReservationCount.setBounds(54, 143, 134, 16);
		contentPanel.add(lblReservationCount);
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton okButton = new JButton("OK");
				//Start to send the name and count to the server
				okButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
					
						if(nameField.getText().equals("")|| countField.getText().equals("")) {
							JOptionPane.showMessageDialog(null,"There is empty field!!");
						}else {
							String data = nameField.getText() + " " + countField.getText();
							Message msg = new Message(MessageType.RESERVE_SEAT, data, null);
							try {
								server.sendMessage(msg);
							} catch (IOException e1) {
								e1.printStackTrace();
							}
							dispose();	
						}
					}
				});
				okButton.setActionCommand("OK");
				buttonPane.add(okButton);
				getRootPane().setDefaultButton(okButton);
			}
			{
				JButton cancelButton = new JButton("Cancel");
				cancelButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						dispose();
					}
				});
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}
	
}
