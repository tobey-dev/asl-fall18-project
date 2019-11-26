package ch.ethz.asltest.Middleware.Environment;

import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Log.Log;

import java.io.IOException;
import java.io.SerializablePermission;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

/*
    This class represents a memcached server and does not contain connection statuses.
    Function getNewConnection returns a new connection to a server.
 */

public class Server extends Environment.Entity{

    private String name;
    private static int serverCount = 0;
    private int serverID = -1;

    private String address;
    private int port;

    private static final Object lock = new Object();

    public Server(String address, int port){
        super();
        this.address = address;
        this.port = port;
        synchronized (lock){
            this.name = "Server-" + serverCount;
            serverID = serverCount;
            serverCount++;
        }
    }

    public String getName() {
        return name;
    }

    public ServerConnection getNewConnection(){
        try {
            Socket socket;
            socket = new Socket(address, port);
            socket.setKeepAlive(Parameters.getBoolean("server_socket_keepalive"));
            socket.setTcpNoDelay(Parameters.getBoolean("server_socket_tcp_nodelay"));
            socket.setReceiveBufferSize(Parameters.getInteger("server_socket_rcv_buffer_size"));
            socket.setSendBufferSize(Parameters.getInteger("server_socket_snd_buffer_size"));
            return new ServerConnection(this, socket);
        } catch (IOException iOException){
            Log.error("[Server] I/O exception encountered when connecting to Server " + address + ":" + port + ": " + iOException.getMessage());
            return null;
        }
    }

    public static class ServerConnection{

        private Server server;
        private Socket socket;

        private boolean answerExpected;

        ServerConnection(Server server, Socket socket){
            this.server = server;
            this.socket = socket;
        }

        public Server getServer() {
            return server;
        }

        public Socket getSocket() {
            return socket;
        }

        public void setAnswerExpected(boolean answerExpected){
            this.answerExpected = answerExpected;
        }

        public boolean getAnswerExpected(){
            return answerExpected;
        }

        public void closeConnection() {
            try {
                if (socket.isConnected()) {
                    socket.close();
                }
                Log.info("ServerConnection to server \"" + server.name + "\" closed");
            } catch (IOException iOException) {
                Log.error("I/O exception encountered when closing SocketChannel \"" + socket.toString() + "\": " + iOException.getMessage());
            }
        }
    }
}
