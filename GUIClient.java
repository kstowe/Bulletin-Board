import java.lang.*;
import java.util.*;
import java.io.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

/**
 * Driver class for running GUIClientPanel. Places the GUIClientPanel in a
 * frame and makes it visible to user.
 */
public class GUIClient {
    public static void main(String[] args)
	throws IOException {

	if(args.length != 2) {
	    System.out.println("Usage: java GUIClient server_port server_ip");
	    System.exit(1);
	}

	int serverPort = 0;
	try {
	    serverPort = Integer.parseInt(args[0]);
	} catch(Exception e) {
	    System.out.println("Error: server_port must be an integer.");
	    System.exit(1);
	}

	JFrame window = new JFrame("Bulletin Board");
	Client client = new Client(serverPort, args[1]);
	GUIClientPanel content = new GUIClientPanel(client);
	window.setContentPane(content);
	window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	window.setLocation(200,200);
	window.setSize(new Dimension(900, 1000));
	window.setVisible(true);
    }
}
