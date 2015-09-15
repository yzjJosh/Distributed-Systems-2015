package client;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JTextField;
import javax.swing.JLabel;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.io.Writer;

public class Reserve extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private JTextField nameField;
	private JTextField countField;
    private Writer writeReserve;
	/**
	 * Create the dialog.
	 */
	public Reserve(Writer writer) {
		writeReserve = writer;
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
						String name = nameField.getText();
						int count = Integer.parseInt(countField.getText());
				        try {
							writeReserve.write(name);
							writeReserve.write(count);
							writeReserve.flush();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
				        
					}
				});
				okButton.setActionCommand("OK");
				buttonPane.add(okButton);
				getRootPane().setDefaultButton(okButton);
			}
			{
				JButton cancelButton = new JButton("Cancel");
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}
	
}
