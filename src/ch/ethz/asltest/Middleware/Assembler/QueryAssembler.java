package ch.ethz.asltest.Middleware.Assembler;

import ch.ethz.asltest.Middleware.Environment.Client;
import ch.ethz.asltest.Middleware.Environment.Server;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.Job;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Log.Statistics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/*
    The QueryAssembler is an Assembler that parses client queries.
    A QueryAssembler is to be used for one SocketChannel only. It copies the data available on a SocketChannel
    to its own ByteBuffer and parses all available data on advance(). Any amount of bytes can be read at a time.
    Once a complete query is parsed, the appropriate Job object is created and the callback is fired carrying the
    Job as argument.
    The query can be written to a server without any intermediate copies with the writeToServer methods.
 */

public class QueryAssembler extends Assembler{

    // This class parses requests which are split over multiple packets.

    private Client submitter;

    private int dataBytesRemaining;

    private ParserState currentState = ParserState.INITIAL;

    private StringBuilder invalidCommandStringBuilder;
    private boolean previousCharacterCR;

    /*
     This array contains 2 values per key contained in the query:
     The first (even ones) is the position where the key starts,
     the second (odd ones) is the length of said key.
     */
    private int[] keyDescriptor;
    // How many keys are currently saved in keyDescriptor
    private int keyCount;

    /*
     During writing to server(s), this indicates which key will be the next to be written.
     use keyDescriptor[2 * nextKeyToWrite] to get its position, keyDescriptor[(2 * nextKeyToWrite) + 1]
     for its length.
      */
    private int nextKeyToWrite;

    private long jobArrivalTime;

    private byte[] SET_COMPARABLE = new byte[]{0x73, 0x65, 0x74}; // set
    private byte[] GET_COMPARABLE = new byte[]{0x67, 0x65, 0x74}; // get



    public QueryAssembler(Client submitter, QueryAssemblerCompletedCallback messageProcessorInterface){
        super(messageProcessorInterface);
        this.submitter = submitter;
        invalidCommandStringBuilder = new StringBuilder();
        keyDescriptor = new int[2 * Parameters.getInteger("assembler_max_keys_readable")];
    }

    /*
  Reads from the given socketchannel and parses the received data.
  The timestamp gets saved when just starting reading on a new message.
 */
    public int readBytes(SocketChannel socketChannel, long timestamp) throws IOException, AssemblerBlockedException {
        if (blocked){
            int startPosition = byteBuffer.position();
            int startLimit = byteBuffer.limit();
            IOException thrownIOException = null;
            int bytesRead = 0;

            /*
             We need one byte of space, since the IOException that indicates
             that the host has closed the connection only gets thrown when
             an actual write can (could) be executed (else, read just returns 0).
             */
            byteBuffer.limit(byteBuffer.position() + 1);
            try {
                bytesRead = socketChannel.read(byteBuffer);
            } catch (IOException iOException){
                thrownIOException = iOException;
            }

            byteBuffer.limit(startLimit);
            byteBuffer.position(startPosition);

            if (thrownIOException != null || (thrownIOException == null && bytesRead < 0)){
                return -1;
            } else {
                throw new AssemblerBlockedException("The assembler is currently blocked as another query associated with the buffer is being processed");
            }
        }

        int startPosition = byteBuffer.position();

        if (startPosition == 0){
            if (timestamp == 0){
                Log.warn("[Assembler] Setting arrival time to 0 even though it should be valid");
            }
            setJobArrivalTime(timestamp);
            Statistics.setClientArrivalTime(socketChannel.socket().getRemoteSocketAddress().toString(), timestamp);
        }

        byteBuffer.limit(byteBuffer.capacity());
        int bytesRead = socketChannel.read(byteBuffer);
        if (bytesRead < 0)
            return bytesRead;
        byteBuffer.limit(byteBuffer.position());
        byteBuffer.position(startPosition);
        advance();
        return bytesRead;
    }

    @Override
    public void release(){
        currentState = ParserState.INITIAL;
        super.release();
    }

    void advance(){
        try{
            byte currentValue;
            while (byteBuffer.hasRemaining()){

                if (byteBuffer.position() >= byteBuffer.limit()){
                    // We ran out of space, increase buffer size
                    Log.error("[QueryAssembler] Buffer space not sufficient, increasing");
                    byteBuffer.rewind();
                    ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() * 2);
                    newBuffer.put(byteBuffer);
                    byteBuffer = newBuffer;
                }

                switch (currentState){
                    case INITIAL:
                        currentValue = byteBuffer.get();
                        dataBytesRemaining = 0;
                        keyCount = 0;
                        nextKeyToWrite = 0;
                        if (currentValue == 0x73){ //s
                            currentState = ParserState.SET_0_1;
                        } else if (currentValue == 0x67){ //g
                            currentState = ParserState.GET_0_1;
                        } else {
                            currentState = ParserState.INVALID;
                            invalidCommandStringBuilder.setLength(0);
                        }
                        break;


                    // set
                    case SET_0_1: case SET_0_2:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, SET_COMPARABLE, ParserState.SET_0_1);
                        break;

                    case SET_0: case SET_1: case SET_2: case SET_3:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x20) { // whitespace
                            currentState = ParserState.values()[currentState.ordinal() + 1];
                        }
                        break;
                    case SET_4:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x20){ // whitespace
                            currentState = ParserState.SET_TAIL;
                        } else if (currentValue == 0x0d) { // CR
                            currentState = ParserState.SET_TAIL;
                        } else {
                            dataBytesRemaining *= 10;
                            dataBytesRemaining += currentValue - 48;
                        }
                        break;
                    case SET_TAIL:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0a){ // LF
                            currentState = ParserState.SET_DATA;
                        }
                        break;
                    case SET_DATA:
                        int advance = Math.min(dataBytesRemaining, byteBuffer.remaining());
                        byteBuffer.position(byteBuffer.position() + advance);
                        dataBytesRemaining -= advance;

                        if (dataBytesRemaining == 0){
                            currentState = ParserState.SET_DATA_TAIL;
                        }
                        break;
                    case SET_DATA_TAIL:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0a){ // LF
                            currentState = ParserState.SET_DONE;
                        }
                        break;

                        // get

                    case GET_0_1: case GET_0_2:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, GET_COMPARABLE, ParserState.GET_0_1);
                        break;

                    case GET_0_3:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x73) { //s
                            currentState = ParserState.GET_0_4;
                        } else if (currentValue == 0x20){ // whitespace
                            keyDescriptor[2 * keyCount] = byteBuffer.position();
                            currentState = ParserState.GET_KEYS;
                        } else {
                            transitionToInvalid("get", currentValue);
                        }
                        break;

                    case GET_0_4:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x20) { // whitespace
                            keyDescriptor[2 * keyCount] = byteBuffer.position();
                            currentState = ParserState.GET_KEYS;
                        } else {
                            transitionToInvalid("gets", currentValue);
                        }
                        break;

                    case GET_KEYS:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0d){ // CR
                            keyDescriptor[(2 * keyCount) + 1] = byteBuffer.position() - keyDescriptor[2 * keyCount] - 1;
                            keyCount++;
                            currentState = ParserState.GET_LF;
                        } else if (currentValue == 0x20){ // whitespace
                            keyDescriptor[(2 * keyCount) + 1] = byteBuffer.position() - keyDescriptor[2 * keyCount] - 1;
                            keyCount++;
                            keyDescriptor[(2 * keyCount)] = byteBuffer.position();
                        }
                        if (((keyCount * 2) + 1) >= keyDescriptor.length){
                            Log.error("[QueryAssembler] Cannot add additional get key to assembler, run out of space (increase assembler_max_keys_readable)");
                        }
                        break;

                    case GET_LF:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0a) { // whitespace
                            currentState = ParserState.GET_DONE;
                        } else {
                            transitionToInvalid("", currentValue);
                        }
                        break;

                        //invalid
                    case INVALID:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0d) { //CR
                            previousCharacterCR = true;
                        } else if (currentValue == 0x0a){ //LF
                            if (previousCharacterCR){
                                Log.error("[QueryAssembler] Received unknown operation that is neither get nor set: " + invalidCommandStringBuilder.toString());
                                previousCharacterCR = false;
                            } else {
                                invalidCommandStringBuilder.append(new String(new byte[]{0x0a}, StandardCharsets.US_ASCII));
                            }
                        } else {
                            byte[] append;
                            if (previousCharacterCR){
                                append = new byte[]{0x0d, currentValue};
                            } else {
                                append = new byte[]{currentValue};
                            }
                            invalidCommandStringBuilder.append(new String(append, StandardCharsets.US_ASCII));
                            previousCharacterCR = false;
                        }
                        break;
                    default:
                        Log.error("[QueryAssembler] Unrecognized state in advance()");
                        break;
                }

                if (currentState == ParserState.SET_DONE || currentState == ParserState.GET_DONE){
                    if (byteBuffer.hasRemaining()){
                        Log.error("[QueryAssembler] byteBuffer has remaining data after fully parsed request");
                    }
                    break;
                }
            }


            if (currentState == ParserState.SET_DONE || currentState == ParserState.GET_DONE){
                byteBuffer.flip();
                blocked = true;

                if (currentState == ParserState.SET_DONE){
                    // Construct a new set job, the full command is in byteBuffer
                    ((QueryAssemblerCompletedCallback) assemblerCompletedCallback).callback(Job.createSetJob((Client) submitter, this, jobArrivalTime));
                }

                if (currentState == ParserState.GET_DONE){
                    // Construct a new get job, the full command is in byteBuffer
                    ((QueryAssemblerCompletedCallback) assemblerCompletedCallback).callback(Job.createGetJob((Client) submitter, this, keyCount, jobArrivalTime));
                }

            }

        } catch (Exception exception) {
            Log.error("[QueryAssembler] An exception occured in message assembly: " + exception.getMessage());
        }
    }

    private void transitionToNextState(byte currentValue, byte[] comparable, ParserState baseState){
        if (comparable[currentState.ordinal() - baseState.ordinal() + 1] == currentValue){
            currentState = ParserState.values()[currentState.ordinal() + 1];
        } else {
            invalidCommandStringBuilder.setLength(0);
            transitionToInvalid(new String(comparable, 0, currentState.ordinal() - baseState.ordinal() + 1, StandardCharsets.US_ASCII), currentValue);
        }
    }

    private void transitionToInvalid(String previousText, byte currentValue){
        if (currentValue == 0x0d){
            previousCharacterCR = true;
            invalidCommandStringBuilder.append(previousText);
        } else {
            invalidCommandStringBuilder.append(previousText).append(new String(new byte[]{currentValue}, StandardCharsets.US_ASCII));
        }
        currentState = ParserState.INVALID;
    }

    private enum ParserState{
        // A new message has begun, we have not yet read anything
        INITIAL,
        // Wait until CRLF and discard (Neither "set", "get" nor "gets")
        INVALID,
        // The supplement n indicates that we are reading the n-th argument (all separated via whitespace)
        // SET_0_1 means we are still reading "set" and expect the "e" character to be next. After SET_0, we wait
        // for a whitespace character and then transition to SET_1.
        // SET_TAIL waits for CRLF
        SET_0_1, SET_0_2,
        SET_0, SET_1, SET_2, SET_3, SET_4, SET_TAIL,
        // SET_DATA is used while reading the data block, SET_DATA_TAIL expects CRLF
        SET_DATA, SET_DATA_TAIL,
        SET_DONE,

        GET_0_1, GET_0_2, GET_0_3, GET_0_4,
        // GET_KEYS waits for CRLF, since there may be an arbitrary amount of keys following the command
        GET_KEYS,
        GET_LF,
        GET_DONE
    }

    /*
    Writes everything from current position to limit to the server.
    After, it flushes and has zero remaining bytes.
     */
    public void writeToServer(Server.ServerConnection serverConnection){
        try {
            Socket socket = serverConnection.getSocket();
            OutputStream outputStream = socket.getOutputStream();
            int position = byteBuffer.position();
            int count = byteBuffer.remaining();
            outputStream.write(byteBuffer.array(), position, count);
            byteBuffer.position(byteBuffer.limit());
            outputStream.flush();
        } catch (IOException iOException){
            Log.error("[QueryAssembler] IOException while writing to server: " + iOException.getMessage());
        }
    }

    // Rewinds the byteBuffer (e.g. to write the same set to multiple servers)
    public void rewind(){
        byteBuffer.rewind();
    }

    /*
    Server write for sharded gets.
    Writes the next keyCount keys (with whitespaces between them) but
    omits both header and tail.
    The next key is stored at the byteBuffer's current position (except for the
    initial write, for which it will be at position zero).
    After, it does NOT flush and its position is unchanged.
     */
    public void writeGetToServer(Server.ServerConnection serverConnection, int  keyCount){
        try {
            Socket socket = serverConnection.getSocket();
            OutputStream outputStream = socket.getOutputStream();
            for (int i = 0; i < keyCount; i++){
                int position = keyDescriptor[2 * nextKeyToWrite];
                int count = keyDescriptor[(2 * nextKeyToWrite) + 1];
                nextKeyToWrite++;
                outputStream.write(byteBuffer.array(), position, count);
                if (i < (keyCount - 1)){
                    // Another key follows, print a whitespace to separate keys
                    outputStream.write(0x20);
                }

            }
        } catch (IOException iOException){
            Log.error("[QueryAssembler] IOException while writing to server: " + iOException.getMessage());
        }
    }

    // This interface is used by Parser to outsource the processing of created jobs
    public interface QueryAssemblerCompletedCallback extends AssemblerCompletedCallback<Job>{
        // No additional methods, only callback<Job> from super
    }

    void setJobArrivalTime(long jobArrivalTime){
        this.jobArrivalTime = jobArrivalTime;
    }

}