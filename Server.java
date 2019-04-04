import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * Driver class for bulletin board server.
 */
public class Server {
    public static final String PRIMARY_ADDRESS = "localhost";
    public static final int PRIMARY_PORT = 10000;
    
    public static void main(String[] args) throws Exception {
	if(args.length < 2) {
	    System.out.println("Usage (replica server): java Server port_for_clients port_for_primary");
	    System.out.println("Usage (primary server): java Server port_for_clients port_for_primary consitency_policy [Nw] [Nr]");
	    System.exit(1);
	}

	int port = Integer.parseInt(args[0]);
	int port2 = Integer.parseInt(args[1]);
	ServerSocket sSock = null;
	
	boolean isPrimary = false;
	int policyNum = 0;
	String policyString;

	if(args.length < 3) {}
	    //policyNum = getPolicyNum();	
	else {
	    policyString = args[2].toLowerCase();
	    isPrimary = true;
	    
	    if(policyString.equals("sequential")) {
		policyNum = 0;
	    } else if(policyString.equals("quorum")) {
		policyNum = 1;
	    } else if(policyString.equals("ryw")) {
		policyNum = 2;
	    } else {
		System.out.println("Not a valid consistency policy. Defaulting to sequential.");
		policyNum = 0;
	    }
	    System.out.println("Policy is: " + policyString);
	} 

	System.out.println("    Listening for clients on port " + args[0]);
	System.out.println("    Listening for servers on port " + args[1]);
	
	ConsistentServer server;
	if(!isPrimary) {
	    server = new ConsistentServer(PRIMARY_PORT, PRIMARY_ADDRESS);
	} else {
	    switch(policyNum) {
	    case 0:
		server = new ConsistentServer(PRIMARY_PORT, PRIMARY_ADDRESS);
		server.setPolicy(ConsistentServer.SEQUENTIAL, isPrimary);
		break;
	    case 1:
		int writeQuorumSize = 0;
		int readQuorumSize = 0;
		if(args.length > 3) {
		    writeQuorumSize = Integer.parseInt(args[3]);
		}
		if(args.length > 4) {
		   readQuorumSize = Integer.parseInt(args[4]);
		}
		server = new ConsistentServer(PRIMARY_PORT, PRIMARY_ADDRESS);
		server.setPolicy(ConsistentServer.QUORUM, writeQuorumSize,
				 readQuorumSize);
		break;
	    case 2:
		server = new ConsistentServer(PRIMARY_PORT, PRIMARY_ADDRESS);
		server.setPolicy(ConsistentServer.RYW, isPrimary);
		break;
	    default:
		server = new ConsistentServer(PRIMARY_PORT, PRIMARY_ADDRESS);
		server.setPolicy(ConsistentServer.SEQUENTIAL, isPrimary);
	    }
	}

	// User must register with primary
	server.registerWithPrimary(port2);
		
	// Replica server socket
	try {
	    sSock = new ServerSocket(port);
	} catch (Exception e) {
	    System.out.println("Error: cannot open socket");
	    System.exit(1);
	}

	// Applies to both sequential and read-your-writes
	server.new CoordinatorConnectionListener(port2).start();
	
	System.out.println("Server is listening...");

	Socket connection;
	while(true) {
	    connection = sSock.accept();
	    while(true) {
		try {
		    server.addToQueue(connection);
		    break;
		} catch(InterruptedException e) {
		    System.out.println("Failed to add connection to queue. Retrying...");
		    continue;
		}
	    }
	}
    }
}

