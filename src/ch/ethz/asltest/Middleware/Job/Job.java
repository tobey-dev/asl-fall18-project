package ch.ethz.asltest.Middleware.Job;

import ch.ethz.asltest.Middleware.Environment.Client;
import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Environment.Server;
import ch.ethz.asltest.Middleware.Assembler.QueryAssembler;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Util.OffsetList;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/*

    Job is the abstract class that represents a query.
    It is extended by SetJob and GetJob, which offer functionality to have the job be written to
    a server with a QueryAssembler that is bound to the Job at creation time (Jobs are created by
    QueryAssembler after being parsed).
    This class also contains all statistical data concerning it (e.g. timestamps).
 */

public abstract class Job {

    protected Client submitter;
    protected QueryAssembler queryAssembler;

    protected int roundRobinIndex;

    // When we can read the first byte
    protected long clientArrivalTime;
    // When the full job has been sent to the server
    protected long[] serverSendTime;
    // When the first data has been read from the server (before parsing)
    protected long[] serverArrivalTime;
    // When the full job has been sent back to the client
    protected long clientSendTime;

    // When the job got put into the queue (just after)
    protected long enqueueTime;
    // When the job got taken from the queue (just after)
    protected long dequeueTime;

    // The size of the queue before adding this job
    protected int enqueueSize;
    // The size of the queue after removing this job
    protected int dequeueSize;

    // The amount of keys that have not been found on the server (zero for sets)
    protected int missCount;

    private Job(Client submitter, QueryAssembler queryAssembler){
        this.submitter = submitter;
        this.queryAssembler = queryAssembler;
        this.serverSendTime = new long[Environment.getServerList().size()];
        this.serverArrivalTime = new long[Environment.getServerList().size()];

    }

    public void setRoundRobinIndex(int roundRobinIndex){
        this.roundRobinIndex = roundRobinIndex;
    }

    public void setServerArrivalTime(long serverReceivedTime, int globalServerIndex){
        this.serverArrivalTime[globalServerIndex] = serverReceivedTime;
    }

    public void setClientSendTime(long clientSendTime){
        this.clientSendTime = clientSendTime;
    }

    public void setDequeueSize(int dequeueSize){
        this.dequeueSize = dequeueSize;
    }

    public void setEnqueueSize(int enqueueSize){
        this.enqueueSize = enqueueSize;
    }

    public void setEnqueueTime(long enqueueTime){
        this.enqueueTime = enqueueTime;
    }

    public void setDequeueTime(long dequeueTime){
        this.dequeueTime = dequeueTime;
    }

    public void reduceMissCount(int missCount){
        this.missCount -= missCount;
        if (this.missCount < 0){
            Log.warn("[Job] Miss count below zero");
        }
    }


    public long getServerArrivalTime(int globalServerIndex){
        return serverArrivalTime[globalServerIndex];
    }

    public long getServerSendTime(int globalServerIndex){
        return serverSendTime[globalServerIndex];
    }

    public long getEnqueueTime(){
        return enqueueTime;
    }

    public long getDequeueTime(){
        return dequeueTime;
    }

    public long getClientSendTime(){
        return clientSendTime;
    }

    public long getClientArrivalTime(){
        return clientArrivalTime;
    }

    public int getEnqueueSize(){
        return enqueueSize;
    }

    public int getDequeueSize(){
        return dequeueSize;
    }

    public int getMissCount(){
        return missCount;
    }

    public Client getSubmitter() {
        return submitter;
    }

    public QueryAssembler getQueryAssembler(){
        return queryAssembler;
    }

    public static SetJob createSetJob(Client submitter, QueryAssembler queryAssembler, long arrivalTime){
        SetJob setJob = new SetJob(submitter, queryAssembler, arrivalTime);

        return setJob;
    }


    public static GetJob createGetJob(Client submitter, QueryAssembler queryAssembler, int keyCount, long arrivalTime){
        GetJob getJob = new GetJob(submitter, queryAssembler, keyCount, arrivalTime);

        return getJob;
    }



    public static class SetJob extends Job{

        SetJob(Client submitter, QueryAssembler queryAssembler, long arrivalTime){
            super(submitter, queryAssembler);
            this.clientArrivalTime = arrivalTime;
        }

        public void writeToServers(OffsetList<Server.ServerConnection> serverConnections){
            serverConnections.setOffset(roundRobinIndex);
            for (Server.ServerConnection serverConnection : serverConnections) {
                queryAssembler.writeToServer(serverConnection);
                serverSendTime[serverConnections.indexOf(serverConnection)] = System.nanoTime();
                queryAssembler.rewind();
                serverConnection.setAnswerExpected(true);
            }
        }

    }

    public static class GetJob extends Job{

        private boolean isMultiEvaluated = false;
        private boolean isMulti = false;

        public final int keyCount;

        private static byte[] header = {0x67, 0x65, 0x74, 0x20};
        private static byte[] tail = {0x0d, 0x0a};

        GetJob(Client submitter, QueryAssembler queryAssembler, int keyCount, long arrivalTime){
            super(submitter, queryAssembler);
            this.keyCount = keyCount;
            this.missCount = keyCount;
            this.clientArrivalTime = arrivalTime;
        }

        /*
         Writes the request to one (non-sharded) or many (sharded) servers.
         If sharded, the method returns the list of servers that received the job
         and will return an answer. The returned list may be equal to serverConnections.
          */
        public void writeToServers(OffsetList<Server.ServerConnection> serverConnections, boolean sharded){
            if (sharded) {
                int serverCount = serverConnections.size();
                int base = (keyCount / serverCount);
                int rest = (keyCount % serverCount);
                int currentKeyCount;

                serverConnections.setOffset(roundRobinIndex);
                for (Server.ServerConnection serverConnection : serverConnections){
                    currentKeyCount = base + (rest > 0 ? 1 : 0);

                    if (currentKeyCount > 0){
                        writeToServer(serverConnection, currentKeyCount);
                        serverSendTime[serverConnections.indexOf(serverConnection)] = System.nanoTime();
                    }

                    serverConnection.setAnswerExpected(currentKeyCount > 0);

                    if (rest > 0){
                        rest--;
                    }
                }

            } else {
                int index = 0;
                for (Server.ServerConnection serverConnection : serverConnections){
                  if (index == roundRobinIndex){
                      queryAssembler.writeToServer(serverConnection);
                      serverSendTime[serverConnections.indexOf(serverConnection)] = System.nanoTime();
                      // No need to rewind the queryAssembler
                      serverConnection.setAnswerExpected(true);
                  } else {
                      serverConnection.setAnswerExpected(false);
                  }
                  index++;
                }
            }
        }

        /*
         Sharded write, which writes the next keyCount requests to the server (including header and tail)
         Doesn't write anything for keyCount 0
          */
        private void writeToServer(Server.ServerConnection serverConnection, int keyCount){
            if (keyCount == 0){
                return;
            }

            // Write header and tail here (the assembler writes only the keys)
            write(header, serverConnection);
            queryAssembler.writeGetToServer(serverConnection, keyCount);
            write(tail, serverConnection);

            // queryAssembler.writeGetToServer doesn't flush, so do it here
            try{
                serverConnection.getSocket().getOutputStream().flush();
            } catch (IOException iOException){
                Log.error("[Job] IOException while writing to server: " + iOException.getMessage());
            }
        }

        private void write(byte[] buffer, Server.ServerConnection serverConnection){
            try {
                Socket socket = serverConnection.getSocket();
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(buffer, 0, buffer.length);
            } catch (IOException iOException){
                Log.error("[Job] IOException while writing to server: " + iOException.getMessage());
            }
        }

        public boolean isMulti(){
            if (isMultiEvaluated)
                return isMulti;

            isMulti = queryAssembler.getOccurrenceCount((byte) 0x20) > 1; //Whitespace

            isMultiEvaluated = true;
            return isMulti;
        }
    }

}
