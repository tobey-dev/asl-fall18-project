package ch.ethz.asltest.Middleware.Threading;

import ch.ethz.asltest.Middleware.Environment.Client;
import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Global.Global;
import ch.ethz.asltest.Middleware.Global.Configuration;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.Job;
import ch.ethz.asltest.Middleware.Job.JobQueue;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Assembler.QueryAssembler;
import ch.ethz.asltest.Middleware.Log.Statistics;

import java.io.IOException;
import java.net.*;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/*
    The NetThread will hold all incoming connections from clients. It holds one QueryAssembler for each client,
    which it uses to parse incoming queries. Once the callback is called from the QueryAssembler with a completed
    Job, the NetThread will put it into the JobQueue.
 */

public class NetThread {

    private static NetThread ourInstance;
    public static NetThread getInstance() {
        return ourInstance;
    }

    public static void initialize() {
        ourInstance = new NetThread();
    }

    private ServerSocketChannel serverSocketChannel;

    private Selector selector;

    private HashMap<SocketChannel, QueryAssembler> queryAssemblerMap;

    private int currentRoundRobinIndex = 0;

    private NetThread(){
        queryAssemblerMap = new HashMap<>();
        setupServerSocketChanel();
    }

    private void setupServerSocketChanel() {
        try {
            serverSocketChannel = ServerSocketChannel.open();

            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, Parameters.getInteger("server_socket_channel_rcv_buffer_size"));
        } catch (IOException iOException) {
            Log.error("[NetThread] I/O exception encountered when configuring server socket channel: " + iOException.getMessage());
        }

        try{
            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getByName(Configuration.getIp()), Configuration.getPort());
            serverSocketChannel.bind(inetSocketAddress, Parameters.getInteger("server_socket_channel_backlog"));
        } catch (UnknownHostException unknownHostException){
            Log.error("[NetThread] Unknown host exception encountered in accept() method: " +unknownHostException.getMessage());
        } catch (IOException iOException){
            Log.error("[NetThread] I/O exception encountered in accept() method: " + iOException.getMessage());
        }

        try{
            selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException iOException){
            Log.error("[NetThread] I/O exception encountered when configuring the nio selector: " + iOException.getMessage());
        }
    }

    public void run() {

        Log.info("[NetThread] NetThread run started");
        long selectorTimeout = Parameters.getLong("netthread_selector_timeout_ms");

        while (true){
            try{

                if (selector.select(selectorTimeout) < 1) {
                    if (Global.isShuttingDown){
                        break;
                    }
                    continue;
                }

                Set<SelectionKey> selectionKeySet = selector.selectedKeys();
                Iterator<SelectionKey> selectionKeyIterator = selectionKeySet.iterator();

                while(selectionKeyIterator.hasNext()) {
                    SelectionKey selectionKey = selectionKeyIterator.next();

                    if(selectionKey.isAcceptable()) {
                        // A connection was accepted by a ServerSocketChannel.
                        acceptNew(selectionKey);
                    } else if (selectionKey.isReadable()) {
                        long arrivalTime = System.nanoTime();
                        // A channel is ready for reading
                        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                        if (!socketChannel.isConnected()){
                            selectionKey.cancel();
                            socketChannel.close();
                        } else {
                            // Check whether the parser is ready
                            if (!queryAssemblerMap.get(socketChannel).isBlocked()) {
                                readChannel(selectionKey, arrivalTime);
                            }
                        }
                    }
                    selectionKeyIterator.remove();
                }

                if (Global.isShuttingDown)
                    break;

            } catch (IOException iOException){
                Log.error("[NetThread] I/O exception encountered when selecting channel: " + iOException.getMessage());
            }
        }

        // Cleanup in separate function
        cleanup();
        Statistics.exportThinkingTimes();
    }

    private void acceptNew(SelectionKey selectionKey){
        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
            SocketChannel socketChannel = serverSocketChannel.accept();

            Log.info("[NetThread] New socket accepted: " + socketChannel.socket().getRemoteSocketAddress().toString());

            try {
                socketChannel.configureBlocking(false);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, Parameters.getBoolean("inferred_server_socket_channel_keepalive"));
                socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, Parameters.getInteger("inferred_server_socket_channel_rcv_buffer_size"));
                socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, Parameters.getInteger("inferred_server_socket_channel_snd_buffer_size"));
                // May lead to weird side effects
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6179351
                //socketChannel.setOption(StandardSocketOptions.SO_LINGER, Parameters.getInteger("inferred_server_socket_channel_linger_timeout_s"));
                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, Parameters.getBoolean("inferred_server_socket_channel_tcp_nodelay"));
            } catch (IOException iOException){
                Log.error("I/O exception encountered when configuring inferred server socket channel: " + iOException.getMessage());
            }

            Client client = new Client(socketChannel);
            Environment.getClientList().add(client);

            SelectionKey clientSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
            clientSelectionKey.attach(client);

            queryAssemblerMap.put(socketChannel, new QueryAssembler(client, new QueryProcessor()));

        } catch (IOException iOException) {
            Log.error("[NetThread] I/O exception encountered when accepting new connection: " + iOException.getMessage());
        }
    }

    private void readChannel(SelectionKey selectionKey, long arrivalTime){
        Client client = (Client) selectionKey.attachment();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        try {
            QueryAssembler queryAssembler = queryAssemblerMap.get(socketChannel);
            if (queryAssembler.readBytes(socketChannel, arrivalTime) < 0){
                // The channel has been closed by the remote host
                removeClient(client);
                selectionKey.cancel();
                Log.info("[NetThread] The connection to a client has been invalidated and removed.");
            }
        } catch (IOException iOException){
            Log.error("[NetThread] I/O exception encountered when reading from client socket channel: " + iOException.getMessage());
            removeClient(client);
            selectionKey.cancel();
            Log.info("[NetThread] The connection to a client has been invalidated and removed.");

        } catch (QueryAssembler.AssemblerBlockedException assemblerBlockedException){
            Log.error("[NetThread] AssemblerBlockedException exception encountered when reading from inferred server socket channel: " + assemblerBlockedException.getMessage());
        }
    }

    private void removeClient(Client client){
        queryAssemblerMap.remove(client.getSocketChannel());
        client.closeConnection();
        Environment.getClientList().remove(client);
    }

    private void cleanup(){
        Log.info("[NetThread] NetThread shutting down...");
        try{
            selector.close();
            serverSocketChannel.close();
        } catch (IOException iOException){
            Log.error("[NetThread] I/O exception encountered when closing serverSocketChannel: " + iOException.getMessage());
        }

        for (Client client : Environment.getClientList()){
            client.closeConnection();
            Log.info("[NetThread] Closed connection to \"" + client.getName() + "\"");
        }
        Log.info("[NetThread] NetThread shutdown done");
    }


    private void incrementRoundRobinIndex(){
        currentRoundRobinIndex = (currentRoundRobinIndex + 1) % Environment.getServerList().size();
    }

    private class QueryProcessor implements QueryAssembler.QueryAssemblerCompletedCallback {
        @Override
        public void callback(Job job) {
            incrementRoundRobinIndex();
            job.setRoundRobinIndex(currentRoundRobinIndex);

            job.setEnqueueSize(JobQueue.getInstance().getQueueSize());
            job.setEnqueueTime(System.nanoTime());
            JobQueue.getInstance().put(job);
        }
    }


}
