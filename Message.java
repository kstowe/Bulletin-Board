import java.util.*;

/**
 * Defines message data structure for representing the messages contained in
 * a bulletin board.
 */
class Message {
    int ID;
    String title;
    String author;
    String body;
    String messageType;
    String replyToMessage;
    Vector replies;

    public String getTitle() {
	return this.title;
    }

    public String getAuthor() {
	return this.author;
    }

    public String getBody() {
	return this.body;
    }

    public String messageType() {
	return this.messageType;
    }

    public String getReplyToMessage() {
	return this.replyToMessage;
    }

    public Message() {
	this(-1, "", "", "", "", "");
    }

    public Message(int ID, String messageType, String title, String author,
		   String body) {
	this.ID = ID;
	this.title = title;
	this.author = author;
	this.body = body;
	this.messageType = messageType;
	replies = new Vector(0,1);
    }

    public Message(int ID, String messageType, String replyToMessage,
		   String title, String author, String body) {
	this.ID = ID;
	this.title = title;
	this.author = author;
	this.body = body;
	this.messageType = messageType;
	this.replyToMessage = replyToMessage;
	replies = new Vector(0,1);
    }

    /**
     * Convert Message object to string format suitbale for displaying to a 
     * user.
     */
    public String toString() {
	String shortenedBody = body;
	if(body.length() > 10) {
	    shortenedBody = body.substring(0, 10) + "...";
	}
	return "" + ID + " Title: " + title + " Author: " + author +
	    " Message: " + shortenedBody;
	
    }

    /**
     * Convert Message object to a string format suitable for sending it over
     * the network.
     */
    public String toNetworkFormat() {
	if(messageType.equals("Post")) {
	    return messageType + "::" + title + "::" + author + "::" + body +
		"::" + ID;
	} else {
	    return messageType + "::" + replyToMessage + "::" + title + "::" +
		author + "::" + body + "::" + ID;
	}
    }

    /**
     * Add the fields of a message to a message object. Essentially a setter
     * method for all the objects attributes all at once.
     * @param data String format of a message
     */
    public void addData(String data) {
	String[] input = data.split("::");
	this.messageType = input[0];
	if(input.length < 6) {
	    this.title = input[1];
	    this.author = input[2];
	    this.body = input[3];
	    this.ID = Integer.parseInt(input[4]);
	} else {
	    this.replyToMessage = input[1];
	    this.title = input[2];
	    this.author = input[3];
	    this.body = input[4];
	    this.ID = Integer.parseInt(input[5]);
	}
    }
}
 
