import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Coordinator thread for maintaining read-your-write consistency. Provides
 * method for granting the token to a replica server.
 */
class RYWCoordinator extends CoordinatorThread {
    /**
     * Token can be granted to replica servers so that they may perform a local
     * write
     */
    private boolean hasToken;
    
    RYWCoordinator() {
	super();
	hasToken = true;
    }

    /**
     * Blocks until token is retrieved from whoever has it. Then grants token
     * to replica server who asked for it. Finally, retrieve token back from
     * replica server along with the most recent message from them.
     */
    public void acquireLock(PrintWriter outStream, BufferedReader inputStream) {
	int success;
	String data;
	String[] input;
	Message update;
	
	// Block until token has been retrieved
	while(!hasToken);
	// Grant token
	hasToken = false;
	System.out.println("Granting token...");
	send(outStream, "GRANT_LOCK");

	System.out.println("Retrieving token...");
	try {
	    while(!inputStream.readLine().equals("UNLOCK"));
	    hasToken = true;
	    data = inputStream.readLine();
	} catch(IOException e) {
	    System.out.println("Error: Could not retrieve lock.");
	    return;
	}
	input = data.split("::");
	++currID;
	if(input.length == 5) {
	    update = new Message(currID, "Post", input[1],
				 input[2], input[3]);
	} else {
	    update = new Message(currID, "Reply", input[1],
				 input[2], input[3], input[4]);
	}
	success = sendUpdatesToReplicas(update.toNetworkFormat());
	send(outStream, success);
    }
}
