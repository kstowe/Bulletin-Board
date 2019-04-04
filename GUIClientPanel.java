import java.lang.*;
import java.util.*;
import java.io.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

/**
 * Front-end GUI application for interacting with bulletin board. Presents
 * a window with 4 buttons corresponding to the 4 bulletin board operations:
 * post, read, choose, reply. The buttons listen for a click and the window
 * is redrawn based on which button was clicked. 
 */
class GUIClientPanel extends JPanel {

    private static final int MAIN_SCREEN = 0, POST_SCREEN = 1, READ_SCREEN = 2,
	                     CHOOSE_SCREEN = 3, REPLY_SCREEN = 4;
    private int currentDisplay;

    /**
     * Text fields for reading user input for each of the components of a 
     * message.
     */
    private JTextField titleField, authorField, replyField, chooseField;
    private JTextArea messageArea;

    /**
     * Client object contains the methods for interacting with the bulletin
     * board server.
     */
    private Client client;

    GUIClientPanel(Client client) {
	this.client = client;

	currentDisplay = MAIN_SCREEN;
	setLayout(new BorderLayout());
	//setMargin(new Insets(3, 3, 3, 3));
	add(new MainDisplay());
    }
    
    private class ButtonListener implements ActionListener {
	public void actionPerformed(ActionEvent evt) {
	    String command = evt.getActionCommand();
	    if(command.equals("Post")) {
		currentDisplay = POST_SCREEN;
		removeAll();
		add(new PostDisplay(false));
		validate();
		repaint();
	    } else if(command.equals("Read") ||
		      command.equals("View Next Messages")) {
		currentDisplay = READ_SCREEN;
		Message[] messages = new Message[5];
		try {
		    messages = client.readMessages();
		} catch(IOException e) {
		    e.printStackTrace();
		    System.exit(1);
		}
		removeAll();
		add(new ReadDisplay(messages));
		validate();
		repaint();
	    } else if(command.equals("Choose")) {
		currentDisplay = CHOOSE_SCREEN;
		removeAll();
		add(new ChooseDisplay(null));
		validate();
		repaint();
	    } else if(command.equals("Reply")) {
		currentDisplay = REPLY_SCREEN;
		removeAll();
		add(new PostDisplay(true));
		validate();
		repaint();
	    } else if(command.equals("Submit")) {
		String newMessage = processInput();
		int res = 0;
		try {
		    res = client.postMessage(newMessage);
		} catch(IOException e) {
		    e.printStackTrace();
		    System.exit(1);
		}
		currentDisplay = MAIN_SCREEN;
		removeAll();
		add(new MainDisplay());
		validate();
		repaint();
	    } else if(command.equals("Fetch Message")) {
		currentDisplay = CHOOSE_SCREEN;
		Message message = new Message();
		int messageNumber = -1;
		try {
		    messageNumber = Integer.parseInt(chooseField.getText());
		    message = client.chooseMessage(messageNumber);
		} catch(Exception e) {
		}
		removeAll();
		add(new ChooseDisplay(message));
		validate();
		repaint();
	    } else if(command.equals("Cancel")) {
		currentDisplay = MAIN_SCREEN;
		client.setCurrentPageNumber(0);
		removeAll();
		add(new MainDisplay());
		validate();
		repaint();
	    }
	}
    }

    private String processInput() {
	String titleInput, authorInput, messageInput;
	String replyInput = "";
	titleInput = titleField.getText();
	authorInput = authorField.getText();
	if(replyField != null) {
	    replyInput = replyField.getText();
	}
	messageInput = messageArea.getText();
	return Client.makeMessage(titleInput, authorInput, replyInput, messageInput);
    }

    /**
     * Panel for displaying a message.
     */
    private class MessageJPanel extends JPanel {
	MessageJPanel(Message message) {
	    if(message != null) {
		setLayout(new BorderLayout());

		JLabel titleLabel, authorLabel, messageLabel;

		JPanel titleAndAuthorPanel = new JPanel();
		titleAndAuthorPanel.setLayout(new BorderLayout());
		add(titleAndAuthorPanel, BorderLayout.NORTH);
	    
		titleLabel = new JLabel("Title: " + message.getTitle());
		authorLabel = new JLabel("Author: " + message.getAuthor());
		titleAndAuthorPanel.add(titleLabel, BorderLayout.NORTH);
		titleAndAuthorPanel.add(authorLabel, BorderLayout.SOUTH);
		
		messageLabel = new JLabel("Message: " + message.getBody());
		add(messageLabel, BorderLayout.SOUTH);
	    }
	}
    }

    /**
     * Panel for displaying the choose operation. Textfield allows user to 
     * enter which message ID they would like to choose. Submit button listens
     * to perform the choose.
     */
    private class ChooseDisplay extends JPanel {
	ChooseDisplay(Message message) {
	    setLayout(new BorderLayout());

	    JPanel selectionContainer = new JPanel();
	    add(selectionContainer, BorderLayout.NORTH);
	    
	    JLabel chooseLabel = new JLabel("Choose Message #");
	    chooseField = new JTextField(20);
	    selectionContainer.add(chooseLabel);
	    selectionContainer.add(chooseField);

	    MessageJPanel messageJPanel = new MessageJPanel(message);
	    add(messageJPanel);

	    JPanel buttonContainer = new JPanel();
	    add(buttonContainer, BorderLayout.SOUTH);

	    JButton nextButton = new JButton("Fetch Message");
	    JButton cancelButton = new JButton("Cancel");
	    ButtonListener buttonListener = new ButtonListener();
	    nextButton.addActionListener(buttonListener);
	    cancelButton.addActionListener(buttonListener);
	    buttonContainer.add(nextButton);
	    buttonContainer.add(cancelButton);
	}
    }

    /**
     * Panel for displaying screen for read operation. Displays 5
     * MessageJPanels, one for each message contained in the read reply.
     */
    private class ReadDisplay extends JPanel {
	ReadDisplay(Message[] messages) {
	    setLayout(new BorderLayout());

	    JPanel messagesContainer = new JPanel();
	    messagesContainer.setLayout(new GridLayout(5, 1));
	    add(messagesContainer, BorderLayout.NORTH);

	    int messagesRead;
	    MessageJPanel messageJPanel;
	    for(Message currentMessage : messages) {
		if(currentMessage != null) {
		    messageJPanel = new MessageJPanel(currentMessage);
		    messagesContainer.add(messageJPanel);
		    messagesRead++;
		}
	    }

	    JPanel buttonContainer = new JPanel();
	    add(buttonContainer, BorderLayout.SOUTH);

	    JButton nextButton = new JButton("View Next Messages");
	    JButton cancelButton = new JButton("Cancel");
	    ButtonListener buttonListener = new ButtonListener();
	    nextButton.addActionListener(buttonListener);
	    cancelButton.addActionListener(buttonListener);
	    buttonContainer.add(nextButton);
	    buttonContainer.add(cancelButton);
	}
    }

    /**
     * Panel for displaying main screen. Displays a button for each of the
     * four bulletin board operations.
     */
    private class MainDisplay extends JPanel {
	MainDisplay() {
	    setBackground(Color.WHITE);
	    setLayout(new BorderLayout());

	    JPanel buttonPanel = new JPanel();
	    add(buttonPanel, BorderLayout.SOUTH);

	    ButtonListener buttonListener = new ButtonListener();

	    JButton postButton = new JButton("Post");
	    postButton.addActionListener(buttonListener);
	    buttonPanel.add(postButton);

	    JButton readButton = new JButton("Read");
	    readButton.addActionListener(buttonListener);
	    buttonPanel.add(readButton);

	    JButton chooseButton = new JButton("Choose");
	    chooseButton.addActionListener(buttonListener);
	    buttonPanel.add(chooseButton);

	    JButton replyButton = new JButton("Reply");
	    replyButton.addActionListener(buttonListener);
	    buttonPanel.add(replyButton);
	}
    }

    /**
    * Panel to display the screen for a post operation. Textfields allow the 
    * user to enter the message title, author, reply number (if necessary) 
    * and contents.
    */
    private class PostDisplay extends JPanel {
	PostDisplay(boolean isReply) {
	    int numLabels = 2;
	    if(isReply) {
		numLabels = 3;
	    }
	    setLayout(new BorderLayout());

	    JPanel labelAndTextPanel = new JPanel();
	    labelAndTextPanel.setLayout(new BorderLayout());
	    add(labelAndTextPanel, BorderLayout.NORTH);
	    
	    JPanel labelPanel = new JPanel();
	    labelPanel.setLayout(new GridLayout(numLabels, 1));
	    labelAndTextPanel.add(labelPanel, BorderLayout.WEST);
	    
	    JLabel titleLabel = new JLabel("Title:");
	    JLabel authorLabel = new JLabel("Author:");
	    labelPanel.add(titleLabel);
	    labelPanel.add(authorLabel);
	    
	    JPanel textFields = new JPanel();
	    textFields.setLayout(new GridLayout(numLabels, 1));
	    labelAndTextPanel.add(textFields, BorderLayout.EAST);
	    
	    titleField = new JTextField(20);
	    authorField = new JTextField(20);
	    textFields.add(titleField);
	    textFields.add(authorField);

	    if(isReply) {
		JLabel replyLabel = new JLabel("Reply to Message #: ");
		labelPanel.add(replyLabel);
		replyField = new JTextField(20);
		textFields.add(replyField);
	    }

	    JPanel messagePanel = new JPanel();
	    messagePanel.setLayout(new BorderLayout());
	    add(messagePanel, BorderLayout.CENTER);
	    
	    JLabel messageLabel = new JLabel("Message:");
	    messagePanel.add(messageLabel, BorderLayout.WEST);
	    messageArea = new JTextArea(8, 20);
	    messagePanel.add(messageArea, BorderLayout.EAST);

	    JButton submitButton = new JButton("Submit");
	    submitButton.addActionListener(new ButtonListener());
	    add(submitButton, BorderLayout.SOUTH);
	}
    }

    public void paintComponent(Graphics g) {
	super.paintComponent(g);
    }
}
