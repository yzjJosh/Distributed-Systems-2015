package client;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Writer;

import javax.swing.JTextField;
import javax.swing.JLabel;

import server.Process;
import message.Message;
import message.MessageType;

public class Search extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private JTextField nameField;
  

	/**
	 * Create the dialog.
	 */
	public Search(final ProcessForClient server) {
	
		setBounds(100, 100, 450, 300);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(null);
		{
			nameField = new JTextField();
			nameField.setBounds(209, 108, 134, 28);
			contentPanel.add(nameField);
			nameField.setColumns(10);
		}
		{
			JLabel lblYourName = new JLabel("Your name:");
			lblYourName.setBounds(94, 114, 103, 16);
			contentPanel.add(lblYourName);
		}
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton okButton = new JButton("OK");
				okButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						String data = nameField.getText() ;						
						if(data.equals("")) {
							JOptionPane.showMessageDialog(null,"There is null field!!");
						}else {
							Message msg = new Message(MessageType.SEARCH_SEAT, data, null);
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
