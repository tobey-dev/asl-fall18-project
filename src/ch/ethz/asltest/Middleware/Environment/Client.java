package ch.ethz.asltest.Middleware.Environment;

import ch.ethz.asltest.Middleware.Log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/*
    A Client is an Environment Entity which represents a single connection to a client.
 */

public class Client extends Environment.Entity{

    private String name;
    private static int clientCount = 0;
    private int clientID = -1;

    // Keep a reference to a socket channel since there is exactly one connection to a client at any time.
    private SocketChannel socketChannel;

    private static final Object lock = new Object();

    public Client(SocketChannel socketChannel){
        super();
        this.socketChannel = socketChannel;
        synchronized (lock){
            this.name = "Client-" + clientCount;
            clientID = clientCount;
            clientCount++;
        }
    }

    public String getName() {
        return name;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void closeConnection() {
        try{
            if (socketChannel.isConnected()) {
                socketChannel.close();
            }
        } catch (IOException iOException) {
            Log.error("I/O exception encountered when closing SocketChannel \"" + socketChannel.toString() + "\": " + iOException.getMessage());
        }
    }
}
