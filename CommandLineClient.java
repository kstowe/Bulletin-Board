import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Front-end application for using a bulletin-board on the command line.
 * Provides command-line interface for post, read, choose, and reply operations.
 * Program exits when user types the word "exit".
 */
public class CommandLineClient {
    public static void main(String[] args)
	throws IOException {

	if(args.length != 2) {
	    System.out.println("Usage: java CommandLineClient server_port server_ip");
	    System.exit(1);
	}

	int serverPort = Integer.parseInt(args[0]);
	String serverIP = args[1];
	Client client = new Client(serverPort, serverIP);
	CommandLineClient clientProgram = new CommandLineClient(client);
	clientProgram.run();
    }

    private static Random rand = new Random();
    private static final int POST = 1, READ = 2, CHOOSE = 3, REPLY = 4,
	EXIT = 5;

    private Client client;

    CommandLineClient(Client client) {
	this.client = client;
    }

    /**
     * Display the interface and scan for input from the user. Terminate program
     * if user enters the word "exit".
     */
    public void run() {
	boolean done = false,  valid = false;
	String input = "";
	int command = -1;

	Scanner scan = new Scanner(System.in);
	while(true) {
	    input = "";
	    valid = false;

	    printMainScreen();

	    while(!valid) {
		input = scan.nextLine();
		try {
		    command = Integer.parseInt(input);
		    valid = true;
		} catch(NumberFormatException e) {
		    System.out.print("Please enter a valid command (1-5): ");
		}
	    }
	    
	    long startTime, endTime;
	    switch(command) {
	    case POST:
		performPost(scan, false);
		break;
	    case READ:
		performRead(scan);
		break;
	    case CHOOSE:
		performChoose(scan);
		break;
	    case REPLY:
		performPost(scan, true);
		break;
	    case EXIT:
		done = true;
		break;
	    default:
		System.out.print("Please enter a valid command (1-5): ");
	    }
	    System.out.println();
	    if(done) {
		break;
	    }
	}
    }

    /**
     * Home screen of the interface, describing how to choose different
     * operations.
     */
    private void printMainScreen() {
	System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
	System.out.println("Bulletin Board Operations");
	System.out.println("    1: Post");
	System.out.println("    2: Read");
	System.out.println("    3: Choose");
	System.out.println("    4: Reply");
	System.out.println("    5: Exit");
	System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
	System.out.print("Enter command number (1-5): ");
    }

    /**
     * Process user input and send post message to server.
     */
    public void performPost(Scanner scan, boolean isReply) {
	int res = 1;
	String title, author, body;
	String msgNum = "";
	String messageType = "Post";

	if(isReply) {
	    messageType = "Reply";
	    System.out.println("Enter the number of the message you would like to respond to:");
	    msgNum = scan.nextLine();
	}
	
	System.out.println("Enter article title:");
	title = scan.nextLine();
	System.out.println("Enter article author:");
	author = scan.nextLine();
	System.out.println("Enter your message:");
	body = scan.nextLine();
	System.out.println("Performing " + messageType + "....");
	emulateDelay();
	try {
	    res = client.postMessage(Client.makeMessage(
					 title, author, msgNum, body));
	} catch(IOException e) {
	}
	if(res == 0) {
	    System.out.println(messageType + ": Success");
	} else {
	    if(isReply)
		System.out.println("Reply: Message with ID " +
				   msgNum + " does not exist");
	    System.out.println(messageType + ": Failed");
	}
    }

    /**
     * Process user input and send read message to server.
     */
    public void performRead(Scanner scan) {
	int pageNumber = 0;
	String response = "N";
	Message[] messages;
	
	System.out.println("Performing read....");
	System.out.println("Bulletin Board Contents: ");
	do {
	    emulateDelay();
	    try {
		messages = client.readMessages();
	    } catch(IOException e) {
		return;
	    }
	    int messagesRead = 0;
	    for(Message currentMessage : messages) {
		if(currentMessage != null) {
		    System.out.println(currentMessage);
		    messagesRead++;
		}
	    }
	    if(messagesRead == 5) {
		System.out.print("See more messages? Y/n: ");
		response = scan.nextLine().toUpperCase();
		pageNumber++;
	    } else {
		break;
	    }
	} while(response.equals("Y"));
	emulateDelay();
	System.out.println("All messages printed.");
    }


    /**
     * Process user input and send choose message to server.
     */
    public void performChoose(Scanner scan) {
	
	System.out.println("Which message would you like to read?");
	String msgNum = scan.nextLine();
	emulateDelay();
	Message message;
	try {
	    message =
		client.chooseMessage(Integer.parseInt(msgNum));
	} catch(Exception e) {
	    System.out.println("Must enter valid integer.");
	    return;
	}
	System.out.println("Message contents:");
	System.out.println(message);
    }

    public static void emulateDelay() {
	int delay = rand.nextInt(300) + 100;
	try {
	    Thread.sleep(delay);
	} catch(Exception e) {
	    e.printStackTrace();
	}
    }
}
