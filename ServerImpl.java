import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.*;

/**
 * Serves as a backend for a bulletin-board application. Main thread listens
 * for client connections on a port specified by the user and queues the 
 * connections as they arrive. Thread pool of workers consumes client
 * connections as they are queued up. Worker threads receive commands from
 * clients for various bulletin board operations: post, reply, read, and choose.
 * The worker threads read/modify the bulletin board appropriately and reports
 * back to the client. 
 */
public class ServerImpl {
    protected static final Logger LOG = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static Random rand = new Random();
    private static final int primaryPort = 10000;
    private static final String primaryIP = "localhost";

    /**
     * Defines the size of the thread pool and the number of connections that can
     * be queued.
     */
    private static final int THREAD_POOL_SIZE = 5;
    private static final int QUEUE_CAPACITY = 10;
    
    /**
     * Internal representation of bulletin board. Stores the messages that have
     * been posted by clients.
     */
    protected Vector bulletinBoard;
    //private boolean hasToken = false;
    //private Policy policy;

    /**
     * Defines the most recent update to the bulletin board. Each time a message
     * is posted to the bulletin board, the version number is increased.
     */
    protected int version = 0;

    /**
     * Queue for storing client connections as they arrive. Thread-safe to
     * support concurrent access.
     */
    private ArrayBlockingQueue<Socket> connectionQueue;

    /**
     * Initializes the bulletin board. Creates thread pool to handle clients.
     */
    public ServerImpl() {
	bulletinBoard = new Vector(5, 2);
	connectionQueue = new ArrayBlockingQueue<Socket>(QUEUE_CAPACITY);

	for(int i = 0; i < THREAD_POOL_SIZE; i++) {
	    ConnectionHandler connectionHandler = new ConnectionHandler();
	    connectionHandler.start();
	}
    }
    
    /**
     * Attempt to add a new socket to the queue. Blocks if queue is full.
     * If interrupted, passes the exception up to the caller.
     */
    public void addToQueue(Socket socket) throws InterruptedException {
	connectionQueue.put(socket);
    }

    /**
     * Used for evaluation of the speed of the program. Emulates the network
     * delays in a wide-area network.
     */
    public static void emulateDelay() {
	int delay = rand.nextInt(300) + 100;
	try {
	    Thread.sleep(delay);
	} catch(InterruptedException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Send a message with a random delay to emulate a wide-area network.
     */
    public static void send(PrintWriter outStream, String msg) {
	emulateDelay();
	outStream.println(msg);
    }

    public static void send(PrintWriter outStream, int msg) {
	emulateDelay();
	outStream.println(msg);
    }

    /**
     * Obtain the contents of the bulletin-board in a string format.
     * Example:
     * 1 Title: Weather Report Author: John Smith Contents: The weather...
     *     2 Title: Crazy weather!! Author: Jane Smith Contents: Wow!
     * 3 Title: New Opening Author: John Doe Contents: There will...
     *
     * In the example, the second line is an example of a reply.
     */
    public String getEntries(Vector list) {
	return getEntries(list, 1);
    }

    /**
     * Recursive utility method for converting the bulletin board to string
     * format. Depth is used to determine the number of indents corresponding
     * to how deep the reply was. Second level messages (replies) have 1
     * indent, third have 2, etc.
     */
    public String getEntries(Vector list, int depth) {
	int size = list.size();
	StringBuilder entries = new StringBuilder();
	String numTabs = "";
	for(int i = 1; i < depth; ++i) {
	    numTabs += "\t";
	}
	for(int i = 0; i < size; ++i) {
	    Message m = (Message)list.get(i);
	    entries.append(numTabs + m.toNetworkFormat() + ",,");
	    entries.append(getEntries(m.replies, depth+1));
	}
	return entries.toString();
    }

    /**
     * Recursive utility method for converting bulletin board to array of messages.
     * In the array that is returned, each messages' replies directly follow it.
     */
    protected Message[] getMessages(Vector list) {
	int size = list.size();
	ArrayList<Message> messageArray = new ArrayList<Message>();
	for(int i = 0; i < size; i++) {
	    Message currentMessage = (Message)list.get(i);
	    messageArray.add(currentMessage);
	    Message[] currentMessageReplies = getMessages(currentMessage.replies);
	    messageArray.addAll(new ArrayList<Message>(
				    Arrays.asList(currentMessageReplies)));
	}

	Object[] objectArray = messageArray.toArray();
	return Arrays.copyOf(messageArray.toArray(), objectArray.length,
			     Message[].class);
    }

    /**
     * Return a subset of the bulletin board, from the parameter to the end,
     * sorted by message id number.
     */
    protected Message[] bulletinBoardCopyOf(int startID) {
	int size = bulletinBoard.size();
	return bulletinBoardCopyOf(startID, size);
    }

    /**
     * Return a subset of the bulletin board sorted by message id number.
     * @param startID First index of the subset of the bulletin board
     * @param endID Last index of the subset of the bulletin board
     * @throws IllegalArgumentException if the startID or endID are out of
     * bounds
     */
    protected Message[] bulletinBoardCopyOf(int startID, int endID) {
	int size = bulletinBoard.size();
	if(startID < 0 || endID > size) {
	    throw new IllegalArgumentException();
	}
	Message[] messageArray = new Message[endID-startID];
	for(int i = startID; i < endID; i++) {
	    messageArray[i] = getArticleByID(i);
	}
	return messageArray;
    }

    public Message getArticleByID(int idNumber) {
	return getArticleByID(bulletinBoard, idNumber);
    }
    
    /**
     * Search the bulletin board for a particular message
     * @param idNumber id number of the message to search for
     * @return the message if found, null if not found
     */
    private Message getArticleByID(Vector list, int idNumber) {
	Message msg;
	int len = list.size();	
	for(int i = 0; i < len; ++i) {
	    msg = (Message)list.get(i);
	    if(msg.ID == idNumber) {
		return msg;
	    }
	    msg = getArticleByID(msg.replies, idNumber);
	    if(msg != null) return msg;
	}
	return null;
    }

    /**
     * Add a message to the local bulletin board. Method is synchronized to
     * guard against concurrent access of the bulletin board by mutliple worker
     * threads. Updates the version to reflect the most recent message seen.
     * @param msg Message to be added. Message components (i.e. title, author,
     *            contents, etc.) are stored in an array
     * @param updateID id number of the message being added
     */
    public synchronized void writeToBulletinBoard(String[] msg, int updateID) {
	System.out.println("Posting message #" + updateID);

	if(updateID > version) {
	    version = updateID;
	}
	
	Message update;
	if(msg[0].equals("POST")) {
	    update = new Message(updateID, "POST", msg[1], msg[2], msg[3]);
	    bulletinBoard.add(update);
	} else {
	    update = new Message(updateID, "REPLY", msg[1], msg[2], msg[3],
				 msg[4]);
	    Message m = getArticleByID(Integer.parseInt(msg[1]));
	    if(m != null) {
		m.replies.add(update);
	    }
	}	
    }

    /**
     * Check that the message ID exists
     * @param replyTo message number in string format
     * @return True if the message with the id exists, false otherwise
     */
    protected synchronized boolean checkValidReply(String replyTo) {
	int msgNum;
	try {
	    msgNum = Integer.parseInt(replyTo);
	    if(msgNum > version) {
		throw new NumberFormatException();
	    }
	} catch(NumberFormatException e) {
	    return false;
	}
	return true;
    }

    /**
     * Perform the post operation. Increment the version number and write
     * to the bulletin board.
     * @return 0 to indicate success of the write
     */
    protected int performPost(String data) {
	String[] input = data.split("::");
	version++;
	writeToBulletinBoard(input, version);
	return 0;
    }

    /**
     * Perform read operation. Read the messages of the bulletin board
     * in chunks of 5 messages at a time. Return the bulletin board contents
     * in string format
     * @param pageNumber Section of interest in the bulletin board. Each page
     * contains 5 messages
     */
    protected Message[] performRead(int pageNumber) {
	Message[] msgArray = getMessages(bulletinBoard);
	//list = list.substring(0, list.length()-2);
	//String[] msgArray = list.split(",,");
	int bbSection = pageNumber*5;
	Message[] subMsgArray = Arrays.copyOfRange(msgArray, bbSection,
				      Math.min(bbSection+5, msgArray.length));
	return subMsgArray;
    }

    /**
     * Perform choose operation. Retrieve a messsage from the bulletin board
     * and return it.
     */
    protected Message performChoose(int messageNumber) {
	Message m = getArticleByID(messageNumber);
	return m;
    }
    
    /**
     * Worker thread for handling a client connection. Threads of this class 
     * compose the thread pool. Each thread polls the connection queue and 
     * pops a connection off the queue when it arrives.
     */
    private class ConnectionHandler extends Thread {
	ConnectionHandler() {
	    setDaemon(true);
	}
	@Override public void run() {
	    Socket client;
	    BufferedReader inputStream;
	    PrintWriter outStream;
	    String data;
	    String[] input;
	    while(true) {
		try {
		    client = connectionQueue.take();
		} catch(InterruptedException e) {
		    continue;
		}
		try {
		    inputStream = new BufferedReader(new InputStreamReader(
							 client.getInputStream()));
		    outStream = new PrintWriter(client.getOutputStream(), true);
		    data = inputStream.readLine();
		    System.out.println(data);
		    input = data.split("::");
		    switch(input[0]) {
		    case "REPLY":
		    case "POST":
			System.out.println("Performing post...");
			int success = performPost(data);
			send(outStream, success);
			break;
		    case "READ":
			System.out.println("Performing read...");
			Message[] msgArray = performRead(Integer.parseInt(input[1]));
			for(int i = 0; i < msgArray.length; i++) {
			    outStream.print(msgArray[i].toNetworkFormat() + "\n");
			}
			send(outStream, "");
			break;
		    case "CHOOSE":
			System.out.println("Performing choose...");
			Message m = performChoose(Integer.parseInt(input[1]));
			if(m == null) {
			    send(outStream, "Does not exist. Message with ID: "
				 + Integer.parseInt(input[1]));
			    break;
			} else {
			    send(outStream, m.toNetworkFormat());
			}
			break;
		    default:
			System.out.println("Invalid command");
			System.exit(0);
		    }
		
		    //Close the input and output streams.
		    inputStream.close();
		    outStream.close();
		    client.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}
    }    
}
