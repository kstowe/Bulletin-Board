import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Provides methods for interacting with back-end bulletin board application. 
 * Implements basic bulletin board operations: post, read, choose, and reply.
 */
public class Client {
    /**
     * IP address and port at which the server is listening
     */
    private String serverIP;
    private int serverPort;

    /**
     * A page refers to a group of 5 messages. This variable tracks the most
     * recent page that has been read during read operations.
     */
    private int currentPageNumber;

    Client(int serverPort, String serverIP) {
	this.serverIP = serverIP;
	this.serverPort = serverPort;
	this.currentPageNumber = 0;
    }

    public void setCurrentPageNumber(int newPageNumber) {
	this.currentPageNumber = newPageNumber;
    }

    /**
     * Ask the server for the message that corresponds to a particular 
     * id number and return that message.
     */
    public Message chooseMessage(int messageNumber) throws IOException {
	Message message = new Message();

	Socket cSock = new Socket(this.serverIP, this.serverPort);
	PrintWriter sendOut = new PrintWriter(cSock.getOutputStream(), true);
	BufferedReader readIn = new BufferedReader(
	    new InputStreamReader(cSock.getInputStream()));
	
	sendOut.println("CHOOSE::" + messageNumber);
	String data = readIn.readLine();
	message.addData(data);

	sendOut.close();
	readIn.close();
	cSock.close();
	return message;
    }

    /**
     * Ask the server for a page of 5 messages in the bulletin board and return
     * that list of messages. It's possible that the Message array that is 
     * returned contains fewer than 5 messages if the bulletin board contains
     * fewer than 5 unread messages.
     */
    public Message[] readMessages() throws IOException {
	Message[] messages = new Message[5];
	
	Socket cSock = new Socket(this.serverIP, this.serverPort);
	PrintWriter sendOut = new PrintWriter(cSock.getOutputStream(), true);
	BufferedReader readIn = new BufferedReader(
	    new InputStreamReader(cSock.getInputStream()));
	
	sendOut.println("READ::" + currentPageNumber);
	int msgsRead = 0;
	String data = readIn.readLine();
	while(data != null && !data.equals("")) {
	    Message message = new Message();
	    message.addData(data);
	    messages[msgsRead] = message;
	    msgsRead++;
	    data = readIn.readLine();
	}

	if(msgsRead == 5) {
	    this.currentPageNumber++;
	} else {
	    this.currentPageNumber = 0;
	}
	
	sendOut.close();
	readIn.close();
	cSock.close();
	return messages;
    }

    /**
     * Ask server to post a message to the bulletin board and return
     * 0 or 1 for success or failure.
     */
    public int postMessage(String newMessage) throws IOException {
	String res = "";
	Socket cSock;
	PrintWriter sendOut;
	BufferedReader readIn;
    
	cSock = new Socket(serverIP, serverPort);
	sendOut = new PrintWriter(cSock.getOutputStream(), true);
	readIn = new BufferedReader(
	    new InputStreamReader(cSock.getInputStream()));

	sendOut.println(newMessage);
	res = readIn.readLine();

	sendOut.close();
	readIn.close();
	cSock.close();
	
	return Integer.parseInt(res);
    }

    /**
     * Convert the components of a message into a string format of a message.
     */
    public static String makeMessage(String title, String author,
				     String replyTo, String body) {
	if(replyTo != "") {
	    return "REPLY::" + replyTo + "::" + title + "::" +
		author + "::" + body;
	} else
	    return "POST::" + title + "::" + author + "::" + body;
    }

    public static String makeMessage(String title, String author,
				     String body) {
	return makeMessage(title, author, "", body);
    }
}
