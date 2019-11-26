package ch.ethz.asltest.Middleware.Threading;

import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Environment.Server;
import ch.ethz.asltest.Middleware.Global.Configuration;
import ch.ethz.asltest.Middleware.Global.Global;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.Job;
import ch.ethz.asltest.Middleware.Job.JobQueue;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Assembler.Assembler;
import ch.ethz.asltest.Middleware.Assembler.ResponseAssembler;
import ch.ethz.asltest.Middleware.Log.Statistics;
import ch.ethz.asltest.Middleware.Result.Result;
import ch.ethz.asltest.Middleware.Result.ResultMerger;
import ch.ethz.asltest.Middleware.Util.OffsetList;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;

/*
    WorkerThreads poll the JobQueue for new Jobs to work on. They send the polled Job to one or many servers (using
    the Job's QueryAssembler) and parse the response(s) in one or more ResponseAssemblers (one per server). Upon having
    parsed all responses, the gathered Result objects will be merged in a ResultMerger and its merged result will
    be written back to the client.
 */

public class WorkerThread extends Thread {

    // int is atomic (volatile) in java
    private static int workerThreadCount = 0;
    private volatile int workerThreadID = -1;

    private static final Object lock = new Object();

    private Selector selector;
    private long selectorTimeout;

    private OffsetList<Server.ServerConnection> serverConnections;

    private HashMap<Server.ServerConnection, ResponseAssembler> responseAssemblerMap;
    private ResultMerger resultMerger;

    Result currentResult;

    private Statistics getStatistics;
    private Statistics setStatistics;

    public WorkerThread(){
        super();
        synchronized(lock){
            workerThreadID = workerThreadCount;
            workerThreadCount++;
        }
        setName("worker" + workerThreadID);
        responseAssemblerMap = new HashMap<>();
        resultMerger = new ResultMerger();
        try{
            selector = Selector.open();
            selectorTimeout = Parameters.getLong("worker_thread_selector_timeout_ms");

        } catch (IOException iOException){
            Log.error("[WorkerThread] Selector could not be opened: " + iOException.getMessage());
        }

        setStatistics = new Statistics(Statistics.StatisticsType.SET);
        getStatistics = new Statistics(Statistics.StatisticsType.GET);

        serverConnections = new OffsetList<>();
        connect();
    }

    // Have this synchronized in order not to have all threads simultaneously attempt connecting
    private synchronized void connect(){
        for (Server s : Environment.getServerList()){
            Server.ServerConnection serverConnection = s.getNewConnection();
            if (serverConnection == null){
                continue;
            }
            serverConnections.add(serverConnection);
            responseAssemblerMap.put(serverConnection, new ResponseAssembler(new ResponseProcessor()));
        }
        if (serverConnections.size() < Environment.getServerList().size()){
            Log.fatal("[WorkerThread] At least one server connection could not be established");
            ShutdownThread.panic(-1);
        }
    }

    private synchronized void disconnect(){
        for (Server.ServerConnection serverConnection : serverConnections){
            serverConnection.closeConnection();
        }
    }

    private OffsetList<Server.ServerConnection> getServerConnections() {
        return serverConnections;
    }

    private void export(){
        Log.info("[WorkerThread] Exporting statistics");
        setStatistics.export();
        getStatistics.export();
    }

    @Override
    public void run() {

        Job currentJob = null;
        JobQueue jobQueue = JobQueue.getInstance();

        while (true){

            try {
                currentJob = jobQueue.poll(Parameters.getLong("worker_thread_poll_timeout_ms"), TimeUnit.MILLISECONDS);
            } catch (InterruptedException interruptedException){
                if (Global.isShuttingDown) {
                    break;
                }
                // NULL is expected on shutdown
                Log.warn("[WorkerThread] Interruption exception encountered in Queue draw method: " + interruptedException.getMessage());
            }

            if (currentJob == null){
                if (Global.isShuttingDown) {
                    break;
                }
                continue;
            }

            currentJob.setDequeueTime(System.nanoTime());
            currentJob.setDequeueSize(jobQueue.getQueueSize());

            if (currentJob instanceof Job.GetJob){
                Job.GetJob getJob = (Job.GetJob) currentJob;
                processGetJob(getJob);
            } else if (currentJob instanceof Job.SetJob){
                Job.SetJob setJob = (Job.SetJob) currentJob;
                processSetJob(setJob);
            } else {
                Log.error("[WorkerThread] Invalid job in RunnableJob");
                continue;
            }

            if (Global.isShuttingDown)
                break;

        }
        Log.info("[WorkerThread] Disconnecting");
        try{
            selector.close();
        } catch (IOException iOException){
            Log.warn("[WorkerThread] Closing the selector has thrown an exception: " + iOException.getMessage());
        }

        disconnect();
        export();
    }


    private void processSetJob(Job.SetJob setJob){

        setJob.writeToServers(serverConnections);
        setJob.getQueryAssembler().release();

        getAndProcessResponses(setJob);

        setStatistics.submit(setJob);
    }

    private void processGetJob(Job.GetJob getJob){

        getJob.writeToServers(serverConnections, Configuration.getReadSharded());
        getJob.getQueryAssembler().release();

        getAndProcessResponses(getJob);

        getStatistics.submit(getJob);
    }

    private void getAndProcessResponses(Job job){

        if (getServerConnections().size() == 0){
            Log.error("[WorkerThread] No servers are available");
            return;
        }

        resultMerger.clear();

        // Don't set the offset (has been set in write to servers)
        Iterator<Server.ServerConnection> serverConnectionIterator = getServerConnections().iterator();
        while (serverConnectionIterator.hasNext()){
            Server.ServerConnection serverConnection = serverConnectionIterator.next();

            if (!serverConnection.getAnswerExpected()){
                continue;
            }

            try{
                ResponseAssembler responseAssembler = responseAssemblerMap.get(serverConnection);
                while (currentResult == null) {

                    if (responseAssembler.readBytes(serverConnection.getSocket()) < 0){
                        // The socket has been closed by the remote host
                        Log.error("[WorkerThread] The connection to " + serverConnection.getServer().getName() + " has been closed by the remote host");
                        serverConnectionIterator.remove();
                        break;
                    }

                    if (Global.isShuttingDown)
                        return;
                }

                // The response has been fully parsed
                if (currentResult != null) {
                    resultMerger.addResult(currentResult);
                    job.setServerArrivalTime(currentResult.getServerArrivalTime(), getServerConnections().indexOf(serverConnection));
                    if (currentResult instanceof Result.ValueResult && job instanceof Job.GetJob){
                        job.reduceMissCount(((Result.ValueResult) currentResult).valueCount);
                    }
                    if (currentResult instanceof Result.ErrorResult ||  currentResult instanceof Result.ServerErrorResult || currentResult instanceof  Result.ClientErrorResult) {
                        if (job instanceof Job.SetJob){
                            setStatistics.submitError(currentResult);
                        } else if (job instanceof  Job.GetJob){
                            getStatistics.submitError(currentResult);
                        }
                    }
                }

                currentResult = null;
            } catch (IOException iOException){
                Log.error("[WorkerThread] IOException on readBytes for responseAssembler: " + iOException.getMessage());
                abandon();
                return;
            } catch (Assembler.AssemblerBlockedException assemblerBlockedException){
                Log.error("[WorkerThread] AssemblerBlockedException on readBytes for responseAssembler: " + assemblerBlockedException.getMessage());
            } catch (ResultMerger.AlreadyMergedException alreadyMergedException){
                Log.error("[WorkerThread] AlreadyMergedException when adding result to ResultMerger: " + alreadyMergedException.getMessage());
            }
        }


        if (getServerConnections().size() == 0){
            Log.info("[WorkerThread] No more servers are available");
            abandon();
            return;
        }

        resultMerger.merge();

        // Since the connection to the submitter is asynchronous, we need to register a selector
        // and wait until the channel is writable to send back the result
        try{
            // Call selectNow() first to clear all cancelled SelectionKeys from previous writes
            selector.selectNow();
            job.getSubmitter().getSocketChannel().register(selector, SelectionKey.OP_WRITE);
        } catch (ClosedChannelException closedChannelException){
            Log.error("[WorkerThread] Channel closed on getAndProcessResponses: " + closedChannelException.getMessage());
            abandon();
            return;
        } catch (IOException iOException){
            Log.error("[WorkerThread] IOException on selector: " + iOException.getMessage());
        }
        try {
            while (resultMerger.hasRemaining()){
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
                        try {
                            SelectionKey selectionKey = selectionKeyIterator.next();

                            if (selectionKey.isWritable()) {
                                resultMerger.writeToClient((SocketChannel) selectionKey.channel());
                            }
                            // Unregister the selector again if everything was written
                            if (!resultMerger.hasRemaining()) {
                                Long timestamp = System.nanoTime();
                                job.setClientSendTime(timestamp);
                                Statistics.setClientSendTime(((SocketChannel) selectionKey.channel()).socket().getRemoteSocketAddress().toString(), timestamp);
                                selectionKey.cancel();
                            }

                            selectionKeyIterator.remove();
                        } catch (ResultMerger.NotMergedException notMergedException){
                            Log.error("[WorkerThread] Not merged exception: " + notMergedException.getMessage());
                        }
                    }

                    if (Global.isShuttingDown)
                        break;

                } catch (IOException iOException){
                    Log.error("[WorkerThread] I/O exception encountered when selecting channel: " + iOException.getMessage());
                    abandon();
                }
            }
        } catch (ResultMerger.NotMergedException notMergedException) {
            Log.error("[WorkerThread] Not merged exception: " + notMergedException.getMessage());
        } finally {
            try{
                resultMerger.release();
            } catch (ResultMerger.NotMergedException notMergedException){
                Log.error("[WorkerThread] Attempted release on ResultMerger threw NotMergedException: " + notMergedException.getMessage());
            }
        }

    }

    private void abandon(){
        // Purge all data
        for (ResponseAssembler responseAssembler : responseAssemblerMap.values()){
            responseAssembler.release();
        }
        try{
            resultMerger.release();
        } catch (ResultMerger.NotMergedException notMergedException){
            Log.error("[WorkerThread] Attempted release on ResultMerger threw NotMergedException during abandon: " + notMergedException.getMessage());
        }
        export();
        Log.info("[WorkerThread] Purged all state data with current Job");
        // No panic here
    }

    private class ResponseProcessor implements ResponseAssembler.ResponseAssemblerCompletedCallback {

        @Override
        public void callback(Result result) {
            WorkerThread.this.currentResult = result;
        }
    }


}
