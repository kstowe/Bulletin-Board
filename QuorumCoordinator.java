import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Coordinator thread for maintaining quorum consistency. Provides methods for
 * arranging read and write quorums.
 */
class QuorumCoordinator extends CoordinatorThread {
    /**
     * Defines size of quorum needed for a write.
     */
    private int writeQuorumSize;

    /**
     * Defines size of quorum needed for a read.
     */
    private int readQuorumSize;

    /**
     * Id number of the last message that was sent.
     */
    private int lastSent = 0;
    
    QuorumCoordinator() {
	this.writeQuorumSize = 0;
	this.readQuorumSize = 0;
    }

    public void setQuorums(int writeQuorumSize, int readQuorumSize) {
	this.writeQuorumSize = writeQuorumSize;
	this.readQuorumSize = readQuorumSize;
    }

    /**
     * Randomly shuffle the list of servers to create a quorum
     */
    public static void shuffleArray(int[] arr) {
	Random rnd = new Random();
	for (int i = arr.length - 1; i > 0; i--) {
	    int index = rnd.nextInt(i + 1);
	    // Simple swap
	    int a = arr[index];
	    arr[index] = arr[i];
	    arr[i] = a;
	}
    }

    /**
     * Assemble a write quorum. Propagate the updates to the write quorum.
     * If the quorum size is not large enough, it is updated.
     */
    public int assembleWriteQuorum(String data) {
	int size = getServerListSize();
	int[] randNums = new int[size];
	int minQuorumSize = size/2 + 1;
	int activeQuorumSize = this.writeQuorumSize;
	if(activeQuorumSize < minQuorumSize) {
	    activeQuorumSize = minQuorumSize;
	}
	UpdateServers[] updateList = new UpdateServers[activeQuorumSize];
	ServerConnection con;
	for(int i = 0; i < size; i++) {
	    randNums[i] = i;
	}
	shuffleArray(randNums);	
	System.out.println("Propagating message to quorum...");
	System.out.print("Members of write quorum: ");
	for(int i = 0; i < activeQuorumSize; ++i) {
	    System.out.print(randNums[i] + " ");
	    con = getServerByID(randNums[i]);
	    updateList[i] = new UpdateServers(con, data);
	    updateList[i].start();
	}
	System.out.println();
	try {
	    for(int i = 0; i < activeQuorumSize; ++i) {
		updateList[i].join();
	    }
	} catch(Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return 0;
    }

    /**
     * Arrange a read quorum. Find the replica server with the highest version.
     * If the current readQuorumSize is not large enough, a default quorum size 
     * will be used.
     * @return the ID of the server in the read quorum with the highest
     * version number
     */ 
    public int assembleReadQuorum() {
	Socket cSock;
	ServerConnection con;
	
	int size = getServerListSize();
	int minQuorumSize = size/2 + 1;
	int activeQuorumSize = this.readQuorumSize;
	if(activeQuorumSize < minQuorumSize) {
	    activeQuorumSize = minQuorumSize;
	}
	int[] versions = new int[activeQuorumSize];
	int[] randNums = new int[size];
	for(int i = 0; i < size; i++) {
	    randNums[i] = i;
	}
	shuffleArray(randNums);

	System.out.println("Querying read quorum for version numbers...");
	System.out.print("Members of read quorum: ");
	try {
	    for(int i = 0; i < activeQuorumSize; i++) {
		System.out.print(randNums[i] + " ");
		con = getServerByID(randNums[i]);
		cSock = new Socket(con.ip, con.port);
		PrintWriter outStream = new PrintWriter(cSock.getOutputStream(),
							true);
		BufferedReader inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		send(outStream, "VERSION_QUERY");
		versions[i] = Integer.parseInt(inputStream.readLine());
	    }
	} catch(Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	System.out.println();
	System.out.println("Max Version: " + randNums[findMax(versions)]);
	return randNums[findMax(versions)];
    }

    /**
     * Let a replica server know that it must send its bulletin board to another
     * server.
     * @param srcServerID - id num of server that must send updates
     * @param destServerID - id num of server that requested quorum read that
     * is receiveing the updates
     */
    public void arrangeReplicaToReplicaUpdate(int srcServerID,
					      int destServerID) {
	Socket cSock;
	BufferedReader inputStream;
	PrintWriter outStream;
	ServerConnection srcServer;
	ServerConnection destServer;
	try {
	    srcServer = getServerByID(srcServerID);
	    destServer = getServerByID(destServerID);
	    cSock = new Socket(srcServer.ip, srcServer.port);
	    outStream = new PrintWriter(cSock.getOutputStream(), true);
	    inputStream = new BufferedReader(
		new InputStreamReader(cSock.getInputStream()));
	    send(outStream, "SERVER_TO_SERVER_TRANSFER::" + destServer.ip + "::" +
			      destServer.port);
	    String resp = inputStream.readLine();
	    cSock.close();
	} catch(Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    /**
     * Synchronize the bulletin boards of all the replicas in
     * the system. Obtain the bulletin boards from writeQuorumSize servers to 
     * ensure all updates are accounted for. Propagate only the new updates 
     * to the replica servers.
     */
    synchronized void synch() {
	ServerConnection con;
	Socket cSock;
	BufferedReader inputStream;
	PrintWriter outStream;
	int size = getServerListSize();
	int[] randNums = new int[size];
	String listOfUpdates;
	String[] updates;
	
	StringBuilder msg = new StringBuilder();
	System.out.println("Last Sent: " + lastSent + ", currID: " + currID);
	
	// Only synchronize when there are new messages
	if(!(lastSent < currID)) {
	    return;
	}

	for(int i = 0; i < size; i++) {
	    randNums[i] = i;
	}
	shuffleArray(randNums);

	Message[] synchList = new Message[currID - lastSent];
	try {
	    // Obtain all updates from 'writeQuorumSize' number of servers
	    for(int i = 0; i < writeQuorumSize; i++) {
		con = getServerByID(randNums[i]);
		cSock = new Socket(con.ip, con.port);
		outStream = new PrintWriter(cSock.getOutputStream(), true);
		inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		send(outStream, "SEND_UPDATES::" + (lastSent+1));
		String data = inputStream.readLine();
		while(data != null && !data.equals("")) {
		    Message message = new Message();
		    message.addData(data);
		    int messageID = message.ID;
		    synchList[messageID-(lastSent+1)] = message;
		    data = inputStream.readLine();
		}
	    }

	    // Propagate the list of updates to all the servers in the system
	    int listSize = getServerListSize();
	    for(int i = 0; i < listSize; i++) {
		con = getServerByID(i);
		cSock = new Socket(con.ip, con.port);
		outStream = new PrintWriter(cSock.getOutputStream(), true);
		inputStream = new BufferedReader(
		    new InputStreamReader(cSock.getInputStream()));
		for(int j = 0; j < synchList.length; j++) {
		    if(synchList[j] != null) {
			send(outStream, synchList[j].toNetworkFormat() + "\n");
		    }
		}
		//send(outStream, msg.substring(0, msg.length()-2));
		cSock.close();
		outStream.close();
		inputStream.close();
	    }
	    // Update the version number last sent to keep track of what messages
	    // have been sent before
	    lastSent = currID;
      } catch(Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }
}
