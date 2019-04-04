import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.TimerTask;
import java.util.Timer;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * Extension of the simple bulletin board server. This server is designed for use
 * in a distributed system. A network of ConsistentServers are coordinated by a
 * primary server to ensure consistency of the bulletin board across the system.
 * The primary server runs a special coordinator thread to communicate with
 * each of the servers in the system. See the class "PrimaryServer" for details.
 * To maintain consistency, a when a ConsistentServer is performing an operation
 * on the bulletin board (post, reply, read, choose), it must undertake some
 * preprocessing. The type of preprocessing required is determined by the 
 * specific consistency policy. That is, to perform an operation, it must go 
 * through it's consistency policy. See SequentialConsistency, QuorumConsistency,
 * and RYWConsistency for details on each policy.
 */
public class ConsistentServer extends ServerImpl {
    public static final int SEQUENTIAL = 0;
    public static final int QUORUM = 1;
    public static final int RYW = 2;
    
    /**
     * Unique identification number of the server so it can be tracked by the
     * primary server.
     */
    private int serverID;

    /**
     * Port number and ip address at which the primary server is listening for 
     * connections from replica servers.
     */
    private int primaryPort;
    private String primaryIP;

    /**
     * Consistency policy object that governs how bulletin board operations are
     * performed.
     */
    private ConsistencyPolicy policy;

    
    /**
     * Initialize the bulletin board and connection queue by calling the parent
     * class constructor. Note that the server does not register with the 
     * primary automatically. This must be called by the user.
     */
    ConsistentServer(int primaryPort, String primaryIP) {
	super();
	this.primaryPort = primaryPort;
	this.primaryIP = primaryIP;
    }

    /**
     * Sets the consistency policy the server will run and whether or not it
     * will run as primary.
     */
    public void setPolicy(int policyNumber, boolean isPrimary) {
	switch(policyNumber) {
	case SEQUENTIAL:
	    this.policy = new SequentialConsistency(isPrimary);
	    break;
	case QUORUM:
	    this.policy = new QuorumConsistency(isPrimary);
	    break;
	case RYW:
	    this.policy = new RYWConsistency(isPrimary);
	    break;
	default:
	    this.policy = new SequentialConsistency(isPrimary);
	}
    }

    /**
     * Sets the policy that the primary server will run. This method is special for
     * quorum consistency and requires the sizes of the read and write quorums as
     * parameters.
     */
    public void setPolicy(int policyNumber, int writeQuorumSize,
			  int readQuorumSize) {
	if(policyNumber == ConsistentServer.QUORUM) {
	    this.policy = new QuorumConsistency(true, writeQuorumSize,
						readQuorumSize);
	}
    }	

    /**
     * Performs post/reply operation. Arranges for message to be posted in a
     * consistent way. Operation goes through the server's consistency policy.
     * @param data Message to be posted in string form
     * @return 0 or -1 for success or failure
     */
    public int performPost(String data) {
	return policy.performPost(data);
    }

    /**
     * Performs read operation. Arranges for bulletin board to be read in a
     * consistent way. Operation goes through the server's consistency policy.
     * @param pageNumber 5 message chunk of bulletin board
     */
    public Message[] performRead(int pageNumber) {
	return policy.performRead(pageNumber);
    }

    /**
     * Performs choose operation. Arranges for message to be read in a 
     * consistent way. Operation goes through the server's consistency policy.
     */
    public Message performChoose(int messageNumber) {
	return policy.performChoose(messageNumber);
    }

    /**
     * Registers with the primary server so it knows that this instance of
     * ConsistentServer is part of the system. The registration info that the
     * server receives is a unique id and the consistency policy that the 
     * coordinator is running.
     * @param port Port on which server will be listening on for future
     * messages from the primary server.
     * @return 0 if register was success, -1 if failure
     */
    public int registerWithPrimary(int port) {
	Socket cSock = null;
	String[] registrationInfo;
	try {
	    cSock = new Socket(primaryIP, primaryPort);
	    BufferedReader inputStream = new BufferedReader(
		new InputStreamReader(cSock.getInputStream()));
	    PrintWriter outStream = new PrintWriter(cSock.getOutputStream(),
						    true);
	    send(outStream, "REGISTER::" + port);
	    String rawData = inputStream.readLine();
	    registrationInfo = rawData.split("::");
	    cSock.close();
	} catch(IOException e) {
	    LOG.log(Level.WARNING, e.getMessage(), e);	    
	    if(cSock != null) {
		try {
		    cSock.close();
		} catch(IOException e2) {
		    LOG.log(Level.WARNING, e2.getMessage(), e2);
		}
	    }
	    return -1;
	}
	this.serverID = Integer.parseInt(registrationInfo[1]);
	int policyNum = Integer.parseInt(registrationInfo[0]);
	this.setPolicy(policyNum, false);
	return 0;
    }

    /**
     * Send a (string) message over the network with a random delay mixed in.
     */
    public static void send(PrintWriter outStream, String msg) {
	emulateDelay();
	outStream.println(msg);
    }

    /**
     * Send a (integer) message over the network with a random delay mixed in.
     */
    public static void send(PrintWriter outStream, int msg) {
	emulateDelay();
	outStream.println(msg);
    }

    /**
     * Thread for listening for connections from the coordinator. When a
     * connection arrives, it is passed off to a worker thread.
     */
    class CoordinatorConnectionListener extends Thread {
	
	private int port;
	
	CoordinatorConnectionListener(int port) {
	    this.port = port;
	}
	
	@Override
	public void run() {
	    try {
		ServerSocket sSock = new ServerSocket(this.port);
		while(true) {
		    new CoordinatorConnectionHandler(sSock.accept()).start();
		}
	    } catch(IOException e) {
		LOG.log(Level.WARNING, e.getMessage(), e);
	    }
	}
    }

    /**
     * Thread for handling a connection with the primary server. Receives a 
     * command from the primary server and takes the appropriate action in
     * response. 
     */ 
    private class CoordinatorConnectionHandler extends Thread {
	
	Socket sSock = null;
	
	CoordinatorConnectionHandler(Socket sSock) {
	    this.sSock = sSock;
	}
	/**
	 * ConsistentServer-Coordinator communication protocol and operations:
	 *     POST/REPLY::title::author::contents::updateID
	 *        Write a message to local bulletin board if it does
	 *        not exist.
	 *     VERSION_QUERY
	 *        Send coordinator current version of bulletin board
	 *     SERVER_TO_SERVER_TRANSFER::replicaServerIP::replicaServerPort
	 *        Send recent messages directly to another replica server.
	 *     SEND_UPDATES::startMessageID
	 *        Send coordinator all messages with id >= startMessageID
	 */
	@Override
	public void run() {
	    LOG.info("Receiving updates from coordinator...");
	    PrintWriter outStream;
	    BufferedReader inputStream;
	    String rawData;
	    try {
		inputStream = new BufferedReader(
		    new InputStreamReader(sSock.getInputStream()));
		outStream = new PrintWriter(
		    sSock.getOutputStream(), true);
		rawData = inputStream.readLine();
	    } catch(IOException e) {
		LOG.log(Level.WARNING, e.getMessage(), e);
		return;
	    }
	    while(rawData != null && !rawData.equals("")) {
		String[] coordinatorInstructions = rawData.split("::");
		int updateID;
		
		switch(coordinatorInstructions[0]) {
		case "POST":
		    updateID = Integer.parseInt(coordinatorInstructions[4]);
		    /* Only make the write if the article does not exist */
		    if(getArticleByID(updateID) == null) {
			writeToBulletinBoard(coordinatorInstructions, updateID);
		    }
		    send(outStream, "OK");
		    break;
		case "REPLY":
		    updateID = Integer.parseInt(coordinatorInstructions[5]);
		    if(getArticleByID(updateID) == null) {
			writeToBulletinBoard(coordinatorInstructions, updateID);
		    }
		    send(outStream, "OK");
		    break;
		case "VERSION_QUERY":
		    send(outStream, version);
		    break;
		case "SERVER_TO_SERVER_TRANSFER":
		    int portNumber = Integer.parseInt(coordinatorInstructions[2]);
		    policy.quorumReplicaToReplicaUpdate(coordinatorInstructions[1],
							portNumber);
		    send(outStream, "OK");
		    break;
		case "SEND_UPDATES":
		    Message[] updateList = bulletinBoardCopyOf(
			Integer.parseInt(coordinatorInstructions[1]));
		    for(int i = 0; i < updateList.length; i++) {
			send(outStream, updateList[i].toNetworkFormat() + "\n");
		    }
		    break;
		default:
		    LOG.warning("Invalid command:" + coordinatorInstructions[0]);
		}
		try {
		    rawData = inputStream.readLine();
		} catch(IOException e) {
		    LOG.log(Level.WARNING, e.getMessage(), e);
		}
	    }
	} 
    }

    /**
     * Nested class defines concept of consistency policy. Defines default
     * implementations of consistent bulletin board operations.
     */
    abstract class ConsistencyPolicy {
	/**
	 * Performs post/reply operation. Forward article to primary server to
	 * notify coordinator of the new message that was posted. Note that
	 * the default post operation in a ConsistentServer does not modify its
	 * own bulletin board. Instead, the primary server will notify it when
	 * its bulletin board can be modified.
	 * @param data Message to be posted in string form
	 * @return - 0 or -1 for success or failure
	 */
	public int performPost(String rawData) {
	    String success = "0";
	    Socket cSock = null;
	    try {
		cSock = new Socket(primaryIP, primaryPort);
		BufferedReader inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		PrintWriter outStream = new PrintWriter(cSock.getOutputStream(),
							   true);
		send(outStream, rawData);
		success = inputStream.readLine();
	    
		cSock.close();
		outStream.close();
	    } catch(IOException e) {
		LOG.log(Level.WARNING, e.getMessage(), e);	    
		if(cSock != null) {
		    try {
			cSock.close();
		    } catch(IOException e2) {
			LOG.log(Level.WARNING, e2.getMessage(), e2);
		    }
		}
		return -1;
	    }
	
	    return Integer.parseInt(success);
	}

	/**
	 * Read operation. Read the messages of the bulletin board
	 * in chunks of 5 messages at a time. Each chunk is called a page.
	 * @param pageNumber Section of interest in the bulletin board. Each page
	 * contains 5 messages
	 * @return Page of bulletin board contents as an array
	 */
	protected Message[] performRead(int pageNumber) {
	    Message[] bulletinBoardAsArray = getMessages(bulletinBoard);
	    int startIndex = pageNumber*5;
	    int endIndex = Math.min(startIndex+5, bulletinBoardAsArray.length);
	    Message[] messagePage = Arrays.copyOfRange(bulletinBoardAsArray,
						       startIndex, endIndex);
	    return messagePage;
	}
	
	/**
	 * Choose operation. Retrieve a messsage from the bulletin board
	 * and return it. Note, the implementation of this method is identical
	 * to that in the Server class.
	 */
	protected Message performChoose(int messageNumber) {
	    Message m = getArticleByID(messageNumber);
	    return m;
	}

	private void quorumReplicaToReplicaUpdate(String ip, int port) { }
    }

    /**
     * Sequential consistency policy contains same operations as default policy.
     */
    class SequentialConsistency extends ConsistencyPolicy {
	SequentialConsistency(boolean isPrimary) {
	    if(isPrimary) {
		SequentialCoordinator coordinator = new SequentialCoordinator();
		coordinator.setPolicy(ConsistentServer.SEQUENTIAL);
		coordinator.start();
	    }
	}
    }

    /**
     * Quorum consistency policy uses default post operation. However, it
     * overrides read and choose operations in order to make read quorums.
     */
    class QuorumConsistency extends ConsistencyPolicy {
	/**
	 * Create quorum consistency policy. Write and read quorum sizes are set
	 * to be 0, which signals to the coordinator to use default values
	 * i.e. (n/2, n/2).
	 */
	QuorumConsistency(boolean isPrimary) {
	    this(isPrimary, 0, 0);
	}

	/**
	 * Creates a quorum consistency policy that runs as coordinator. Begins
	 * the syncrhonization thread to periodically synchronize servers.
	 */
	QuorumConsistency(boolean isPrimary, int writeQuorumSize,
			  int readQuorumSize) {
	    if(isPrimary) {
		QuorumCoordinator coordinator = new QuorumCoordinator();
		coordinator.setPolicy(ConsistentServer.QUORUM);
	    
		coordinator.setQuorums(writeQuorumSize, readQuorumSize);
		/* Run synch in background every 30 seconds */
		LOG.info("Setting up synch");
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
			    LOG.info("Synchronizing...");
			    coordinator.synch();
			    LOG.info("Synchronization complete.");
			}
		    }, 30*1000, 30*1000);
	
		coordinator.start();
	    }
	}

	/**
	 * Layers quorum consistency onto the default read operation. Makes a
	 * quorum read to ensure it has the most updated version.
	 */
	@Override
	public Message[] performRead(int pageNumber) {
	    int success = askPrimaryForReadQuorum();
	    if(success == -1) {
		return null;
	    } else {
		return super.performRead(pageNumber);
	    }
	}
	
	/**
	 * Layers quorum consistency onto the default choose operation. Makes a
	 * quorum read to ensure it has the most updated version.
	 */
	@Override
	public Message performChoose(int messageNumber) {
	    int success = askPrimaryForReadQuorum();
	    if(success == -1) {
		return null;
	    } else {
		return super.performChoose(messageNumber);
	    }
	}

	/**
	 * Request the primary to assemble a read quorum to find the most recent
	 * posts. Blocks until the primary says it has updated the server local
	 * bulletin board and that it is okay to proceed with the read.
	 */
	private int askPrimaryForReadQuorum() {
	    Socket cSock = null;
	    try {
		cSock = new Socket(primaryIP, primaryPort);
		BufferedReader inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		PrintWriter outStream = new PrintWriter(cSock.getOutputStream(),
							true);
		send(outStream, "QUORUM_READ::"+serverID);
		while(!inputStream.readLine().equals("OK"));
	    } catch(IOException e) {
		LOG.log(Level.WARNING, e.getMessage(), e);	    
		if(cSock != null) {
		    try {
			cSock.close();
		    } catch(IOException e2) {
			LOG.log(Level.WARNING, e2.getMessage(), e2);
		    }
		}
		return -1;
	    }
	    return 0;
	}

	/**
	 * Send a list of updates directly to another replica server. Blocks 
	 * until the replica server says it has updated its local bulletin board
	 * @param ip IP address of replica server recipient
	 * @param port Port on which replica server recipient is listening
	 * @return 0 for success, -1 for failure
	 */
	private int quorumReplicaToReplicaUpdate(String ip, int port) {
	    Socket cSock = null;
	    try {
		LOG.info("Sending updates to server at IP: " + ip +
				   " and port: " + port);
		cSock = new Socket(ip, port);
		PrintWriter outStream = new PrintWriter(cSock.getOutputStream(),
							true);
		BufferedReader inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		Message[] updateList = bulletinBoardCopyOf(1);
		for(int i = 0; i < updateList.length; i++) {
		    send(outStream, updateList[i].toNetworkFormat() + "\n");
		}
		/* Block until the server acknowledges the message */
		while(!inputStream.readLine().equals("OK"));
		cSock.close();
		outStream.close();
		inputStream.close();   
	    } catch(IOException e) {
		LOG.log(Level.WARNING, e.getMessage(), e);	    
		if(cSock != null) {
		    try {
			cSock.close();
		    } catch(IOException e2) {
			LOG.log(Level.WARNING, e2.getMessage(), e2);
		    }
		}
		return -1;
	    }
	    
	    return 0;
	}
    }

    class RYWConsistency extends ConsistencyPolicy {
	/**
	 * Token specifies whether local replica server is allowed to have write-
	 * access to its local bulletin board.
	 */
	private boolean hasToken;

	/*
	RYWConsistency(int primaryPort, String primaryIP, int port) {
	    super(primaryPort, primaryIP, port);
	    this.hasToken = false;
	}

	RYWConsistency(int primaryPort, String primaryIP, int port,
		       boolean isPrimary) {
	    this(primaryPort, primaryIP, port);
	    if(isPrimary) {
		CoordinatorThread coordinator = new RYWCoordinator();
		coordinator.setPolicy(2);
		coordinator.start();
	    }
	}
	*/

	RYWConsistency() {
	    this.hasToken = false;
	}
	
	/**
	 * Creates a read-your-write consistency policy that runs as coordinator.
	 */
	RYWConsistency(boolean isPrimary) {
	    this();
	    if(isPrimary) {
		RYWCoordinator coordinator = new RYWCoordinator();
		coordinator.setPolicy(ConsistentServer.RYW);
		coordinator.start();
	    }
	}

	/**
	 * Writes a message to the local bulletin-board. Note that this version of
	 * post does not forward the message to the primary server. Instead, it
	 * just makes the write locally.
	 */
	@Override
	public int performPost(String data) {
	    String success = "";
	    String[] input = data.split("::");
	
	    success = RYWlocalWrite(input);

	    return Integer.parseInt(success);
	}

	/**
	 * Overlays read-your-write consistency over the default read operation.
	 */
	@Override
	public Message[] performRead(int pageNumber) {
	    RYWcheckForUpdates();
	    return super.performRead(pageNumber);
	}

	/**
	 * Overlays read-your-write consistency over the default choose 
	 * operation.
	 */
	@Override
	public Message performChoose(int messageNumber) {
	    RYWcheckForUpdates();
	    return super.performChoose(messageNumber);
	}

	/**
	 * For reads, we must ensure all other messages written by a user are
	 * available. Ask the primary server whether the replica is missing any
	 * messages.
	 */
	private void RYWcheckForUpdates() {
	    Socket cSock = null;
	    PrintWriter outStream;
	    BufferedReader inputStream;
	    String response;
	    try {
		LOG.info("Checking coordinator for updates...");
		cSock = new Socket(primaryIP, primaryPort);
		inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		outStream = new PrintWriter(cSock.getOutputStream(), true);
		send(outStream, "CHECK::"+version);
		while(!inputStream.readLine().equals("OK")) {
		    send(outStream, "CHECK::"+version);
		}
	    } catch(IOException e) {
		e.printStackTrace();
		System.exit(1);
	    }
	}

	/**
	 * Obtain the token from the primary, perform the write locally, and send
	 * the token back along with the new update to the primary.
	 * @return - success or failure
	 */
	private String RYWlocalWrite(String[] update) {
	    Socket cSock = null;
	    PrintWriter outStream;
	    BufferedReader inputStream;

	    String lock = "";
	    Message msg;
	    String success = "";

	    try {
		cSock = new Socket(primaryIP, primaryPort);
		inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		outStream = new PrintWriter(cSock.getOutputStream(), true);
	
		// Obtain token
		LOG.info("Requesting lock from coordinator...");
		send(outStream, "ACQUIRE_LOCK");
		lock = inputStream.readLine();
		if(lock.equals("GRANT_LOCK")) {
		    hasToken = true;
		}
		LOG.info("Received lock");

		 /* The local replica server is the first to make the update and
		    must choose an id. */
		version++;
		int updateID = version;
		writeToBulletinBoard(update, updateID);

		// Send update and return token
		hasToken = false;
		LOG.info("Returning lock to coordinator...");
		send(outStream, "UNLOCK");
		msg = getArticleByID(version);
		send(outStream, msg.toNetworkFormat());
		success = inputStream.readLine();

		cSock.close();
		outStream.close();
	    } catch(IOException e) {
		e.printStackTrace();
		System.exit(1);
	    }
	    return success;
	}
    }
}


