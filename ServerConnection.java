/**
 * Data structure for representing the identification of a replica server.
 */
public class ServerConnection {
    String ip;
    int port;
    int version;

    public ServerConnection(String ip, int port) {
	this.ip = ip;
	this.port = port;
	this.version = 0;
    }
}
