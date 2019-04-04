import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Provides implementation of coordinating thread that runs in the primary
 * server. The coordinator communicates with each replica server in the system
 * to ensure consistency. The cooridnator thread listens for connections from 
 * the replica servers in the system. When a connection arrives it places the 
 * connection in a queue. A thread pool of worker threads handle each connection
 * connection that is placed on the queue. Worker threads respond to different
 * commands from the replica servers: register, policy query, post message, 
 * arrange a quorum, and ask for a token. Each command is detailed further below.
 */
public class CoordinatorThread extends Thread {
    /**
     * List of replica servers that are active in the system.
     */
    private Vector serverList = new Vector(5, 2);

    /**
     * Integer representation of active consistency policy.
     */
    private int policy;

    /**
     * Counter of number of servers in the system. Used to assign a unique id
     * to new servers being added.
     */
    private int serverIDCounter = 0;

    /**
     * Counter of number of messages in the bulletin board. Used to assign a
     * unique id number to new messages being added.
     */
    protected int currID = 0;

    /**
     * Random instance for generating random delays. See emulateDelay();
     */
    private static Random rand = new Random();

    /**
     * Port on which the coordinator will listen for replica server connections.
     */
    private static final int primaryPort = 10000;

    /**
     * Defines the size of the thread pool and the number of connections that can
     * be queued.
     */
    private static final int THREAD_POOL_SIZE = 5;
    private static final int QUEUE_CAPACITY = 10;

    /**
     * Queue for storing replica server connections as they arrive. Thread-safe
     * to support concurrent access.
     */
    private ArrayBlockingQueue<Socket> replicaServerQueue;

    /**
     * Initialize server list and connection queue.
     */
    CoordinatorThread() {
	serverList = new Vector(5, 2);
	replicaServerQueue = new ArrayBlockingQueue<Socket>(QUEUE_CAPACITY);
    }

    /**
     * Create and run worker threads in thread pool. Then listen for connections
     * from replica servers and put the connections in the queue.
     */
    @Override
    public void run() {
	for(int i = 0; i < THREAD_POOL_SIZE; i++) {
	    ServerQueryHandler serverQueryHandler = new ServerQueryHandler();
	    serverQueryHandler.start();
	}
	
	ServerSocket sSockForServers = null;
	
	try {
	    sSockForServers = new ServerSocket(primaryPort);
	} catch (Exception e) {
	    System.out.println("Error: cannot open socket");
	    System.exit(1);
	}
	Socket replicaServer;
	try {
	    while(true) {
		replicaServer = sSockForServers.accept();
		replicaServerQueue.put(replicaServer);
	    }
	} catch(IOException e) {
	    System.exit(0);
	} catch(InterruptedException e) {
	}
    }
    
    public void setPolicy(int policy) {
	this.policy = policy;
    }

    public int getServerListSize() {
	return serverList.size();
    }

    /**
     * Access the replica server that is identified by a particular id.
     */
    public ServerConnection getServerByID(int id) {
	return (ServerConnection)serverList.get(id);
    }

    /**
     * Used for evaluation of the speed of the program. Emulates the network
     * delays in a wide-area network.
     */
    public static void emulateDelay() {
	int delay = rand.nextInt(300) + 100;
	try {
	    Thread.sleep(delay);
	} catch(Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * Wrapper method for sending (string) messages with a random delay.
     */
    public static void send(PrintWriter outStream, String msg) {
	emulateDelay();
	outStream.println(msg);
    }

    /**
     * Wrapper method for sending (integer) messages with a random delay.
     */
    public static void send(PrintWriter outStream, int msg) {
	emulateDelay();
	outStream.println(msg);
    }

    /**
     * Determine the server with the highest version number.
     * @return Highest version number that any replica server has.
     */
    public static int findMax(int[] arr) {
	int maxServer = 0;
	int maxVers = 0;
	for(int i = 0; i < arr.length; i++) {
	    if(arr[i] > maxVers) {
		maxVers = arr[i];
		maxServer = i;
	    }
	}
	return maxServer;
    }

    /**
     * Register a new replica server to maintain a list of all servers.
     * @param ip IP address the replica is located at
     * @param port Port at which the replica is listening for updates
     * @return The unique ID number which identifies the replica
     */
    public synchronized int registerReplicaServer(String ip, int port) {
	serverList.add(new ServerConnection(ip, port));
	int id = serverIDCounter;
	++serverIDCounter;
	return id;
    }

    /**
     * Assemble the components of a message stored in an array to a string that
     * is suitable for sending over the network.
     * @param msgType Type of message: either "post" or "reply"
     * @param msgParts Array containing components of a message including title,
     * author, message contents
     * @return string format of a message
     */
    public static String makeMessageToString(String msgType, String[] msgParts) {
	String msgString;
	if(msgType.toUpperCase().equals("POST")) {
	    msgString = "POST::" + msgParts[1]+"::"+ msgParts[2]+"::"+
		msgParts[3];
	} else {
	    msgString = "REPLY::"+msgParts[1]+"::"+msgParts[2]+"::"+ msgParts[3]+
		"::"+msgParts[4];
	}
	return msgString;
    }

    /**
     * Make a post message to propagate to replicas.
     */
    public synchronized int makePost(String[] msg) {
	int success;
	++currID;
	String msgString = makeMessageToString("POST", msg) + "::" + currID;

	success = sendUpdatesToReplicas(msgString);
	return success;
    }

    /**
     * Make a reply message to propagate to replicas
     */
    public synchronized int makeReply(String[] msg) {
	int success;
	++currID;
	String msgString = makeMessageToString("REPLY", msg) + "::" + currID;
	
	success = sendUpdatesToReplicas(msgString);
	return success;
    }

    /**
     * Propagate a given update to all the replicas in the system. The 
     * communication with each server will be handled by a separate thread.
     * @return - success or failure
     */
    synchronized int sendUpdatesToReplicas(String data) {
	int size = serverList.size();
	UpdateServers[] updateList = new UpdateServers[size];
	ServerConnection con;

	System.out.println("Propagating new message to replica servers...");
	for(int i = 0; i < size; i++) {
	    con = (ServerConnection)serverList.get(i);
	    if(currID != con.version) {
		updateList[i] = new UpdateServers(con, data);
		updateList[i].start();
	    }
	}
	try {
	    for(int i = 0; i < size; i++) {
		if(updateList[i] != null) {
		    updateList[i].join();
		}
	    }
	} catch(Exception e) {
	    return 1;
	}
	return 0;
    }

    /**
     * Method signatures for methods used in worker threads but only implemented
     * in child classes.
     */
    protected void arrangeReplicaToReplicaUpdate(int srcServerID,
						 int destServerID) { }

    protected int assembleReadQuorum() { return 0; }

    protected void acquireLock(PrintWriter outStream,
			       BufferedReader inputStream) { }

    /**
     * Thread for propagating updates to the replicas servers.
     */
    class UpdateServers extends Thread {
	ServerConnection con;
	String data;
	UpdateServers(ServerConnection con, String data) {
	    this.con = con;
	    this.data = data;
	}
	@Override
	public void run() {
	    Socket cSock;
	    BufferedReader inputStream;
	    PrintWriter outStream;
	    try {
		cSock = new Socket(con.ip, con.port);
		outStream = new PrintWriter(cSock.getOutputStream(), true);
		inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		send(outStream, data);
		inputStream.readLine();
	    } catch(Exception e) {
		e.printStackTrace();
		System.exit(1);
	    }
	}
    }
    
    /**
     * Worker thread for handling queries from replica servers. Each worker
     * thread polls the queue of replica server connections. When a connection
     * is available, it services the connection appropriately depending on the 
     * query.
     */
    private class ServerQueryHandler extends Thread {
	ServerQueryHandler() {
	    setDaemon(true);
	}
	@Override
	public void run() {
	    Socket replicaServer;
	    BufferedReader inputStream;
	    PrintWriter outStream;
	    int success;
	    
	    while(true) {
		try {
		    replicaServer = replicaServerQueue.take();
		} catch(InterruptedException e) {
		    continue;
		}
		
		try {
		    inputStream = new BufferedReader(
			new InputStreamReader(replicaServer.getInputStream()));
		    outStream = new PrintWriter(replicaServer.getOutputStream(),
						true);
		    String data = inputStream.readLine();
		    String[] input = data.split("::");
		    switch(input[0]) {
		    case "POST":
			success = makePost(input);
			send(outStream, success);
			break;
		    case "REGISTER":
			String ip =
			    (replicaServer.getInetAddress()).getHostAddress();
		        int id =
			    registerReplicaServer(ip, Integer.parseInt(input[1]));
			System.out.println("Registering server #" + id);
			// Send the server the consistency policy and server ID
			send(outStream, policy + "::" + id);
			break;
		    case "POLICY":
			send(outStream, policy);
			break;
		    case "REPLY":
			success = makeReply(input);
			send(outStream, success);
			break;
		    case "QUORUM_READ":
			int maxServer = assembleReadQuorum();
			arrangeReplicaToReplicaUpdate(maxServer,
						      Integer.parseInt(input[1]));
			send(outStream, "OK");
			break;
		    case "CHECK":
			// Check if there are any updates that post-date the
			// querying servers version number
			int serverVersion = Integer.parseInt(input[1]);
			if(serverVersion != currID) {
			    send(outStream, "WAIT");
			} else {
			    send(outStream, "OK");
			}
			break;
		    case "ACQUIRE_LOCK":
			acquireLock(outStream, inputStream);
			break;
		    default:
			System.out.println("Invalid argument: " + input[0]);
			System.exit(1);
		    }
		    replicaServer.close();
		} catch(IOException e) {
		    System.exit(1);
		}
	    }
	}
    }

    class monitorQuit extends Thread {
	@Override
	public void run() {
	    BufferedReader inFromClient = new BufferedReader(
		new InputStreamReader(System.in));
	    String st = null;
	    while(true){
		try{
		    st = inFromClient.readLine();
		} catch (IOException e) {
		}
		if(st.equalsIgnoreCase("exit")){
		    System.exit(0);
		}
	    }
	}
    }    
}

