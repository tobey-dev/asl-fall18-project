package ch.ethz.asltest.Middleware.Assembler;

import ch.ethz.asltest.Middleware.Environment.Client;
import ch.ethz.asltest.Middleware.Environment.Server;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.Job;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Result.Result;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/*
    The ResponseAssembler is an Assembler that parses server responses.
    A ResponseAssembler is to be used for one SocketChannel only. It copies the data available on a SocketChannel
    to its own ByteBuffer and parses all available data on advance(). Any amount of bytes can be read at a time.
    Once a complete response is parsed, the appropriate Result object is created and the callback is fired carrying the
    Result as argument.
    The Result can be written to a server without any intermediate copies with the writeToServer methods.
 */

public class ResponseAssembler extends Assembler{

    private int dataBytesRemaining;

    private StringBuilder invalidCommandStringBuilder;
    private StringBuilder errorMessageStringBuilder;
    private ParserState currentState = ParserState.INITIAL;

    private boolean previousCharacterCR;


    /*
     This variable contains the position that is to be the limit once writing to a client.
     For all result types except VALUE, this is the end of the buffer.
     For VALUE, it's the end of all VALUE lines (including datablocks and CRLF),
     such that continuously writing out these results, they patch together to a merged response.
      */
    private int writingLimit;

    private long serverArrivalTime;

    private int valueCount;

    private byte[] STORED_COMPARABLE = new byte[]{0x53, 0x54, 0x4f, 0x52, 0x45, 0x44, 0x0d, 0x0a};// STORED\CR\LF
    private byte[] ERROR_COMPARABLE = new byte[]{0x45, 0x52, 0x52, 0x4f, 0x52, 0x0d, 0x0a}; //ERROR\CR\LF
    private byte[] SERVER_ERROR_COMPARABLE = new byte[]{0x53, 0x45, 0x52, 0x56, 0x45, 0x52, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x20}; // SERVER_ERROR\space
    private byte[] CLIENT_ERROR_COMPARABLE = new byte[]{0x43, 0x4c, 0x49, 0x45, 0x4e, 0x54, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x20}; //CLIENT_ERROR\space
    private byte[] VALUE_COMPARABLE = new byte[]{0x56, 0x41, 0x4c, 0x55, 0x45, 0x20}; // VALUE\space
    private byte[] END_COMPARABLE = new byte[]{0x45, 0x4e, 0x44, 0x0d, 0x0a}; //END\CR\LF



    public ResponseAssembler(ResponseAssemblerCompletedCallback responseAssemblerCompletedCallback){
        super(responseAssemblerCompletedCallback);
        invalidCommandStringBuilder = new StringBuilder();
        errorMessageStringBuilder = new StringBuilder();
    }


    //Reads from the given socket and parses the received data
    public int readBytes(Socket socket) throws IOException, AssemblerBlockedException {
        if (blocked){
            throw new AssemblerBlockedException("The assembler is currently blocked as another query associated with the buffer is being processed");
        }
        InputStream inputStream = socket.getInputStream();
        byteBuffer.limit(byteBuffer.capacity());
        int startPosition = byteBuffer.position();

        int bytesRead = inputStream.read(byteBuffer.array(), byteBuffer.position(), byteBuffer.capacity() - byteBuffer.position());

        if (startPosition == 0){
            setServerArrivalTime(System.nanoTime());
        }

        if (bytesRead < 0){
            return bytesRead;
        }

        byteBuffer.limit(startPosition + bytesRead);
        advance();
        return bytesRead;
    }

    @Override
    public void release(){
        currentState = ParserState.INITIAL;
        super.release();
    }

    @Override
    void advance() {
        try{
            byte currentValue;
            while (byteBuffer.hasRemaining()){

                if (byteBuffer.position() >= byteBuffer.limit()){
                    // We ran out of space, increase buffer size
                    Log.warn("[ResponseAssembler] Buffer space not sufficient, increasing");
                    byteBuffer.rewind();
                    ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() * 2);
                    newBuffer.put(byteBuffer);
                    byteBuffer = newBuffer;
                }

                switch (currentState) {
                    case INITIAL:
                        currentValue = byteBuffer.get();
                        dataBytesRemaining = 0;
                        writingLimit = 0;
                        valueCount = 0;
                        if (currentValue == 0x53) { // S
                            currentState = ParserState.STORED__OR__SERVER_ERROR;
                        } else if (currentValue == 0x45) { // E
                            currentState = ParserState.ERROR__OR__END;
                        } else if (currentValue == 0x43) { // C
                            currentState = ParserState.CLIENT_ERROR_0_1;
                        } else if (currentValue == 0x56) { // V
                            currentState = ParserState.VALUE_0_1;
                        } else {
                            currentState = ParserState.INVALID;
                            invalidCommandStringBuilder.setLength(0);
                        }
                        break;

                    case STORED__OR__SERVER_ERROR:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x54) { // T
                            currentState = ParserState.STORED_0_2;
                        } else if (currentValue == 0x45) { // E
                            errorMessageStringBuilder.setLength(0);
                            currentState = ParserState.SERVER_ERROR_0_2;
                        } else {
                            transitionToInvalid("S", currentValue);
                        }
                        break;

                    case ERROR__OR__END:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x52) { // R
                            currentState = ParserState.ERROR_0_2;
                        } else if (currentValue == 0x4e){ // N
                            writingLimit = byteBuffer.position() - 2;
                            currentState = ParserState.END_0_2;
                        } else {

                        }
                        break;

                    case STORED_0_2: case STORED_0_3: case STORED_0_4: case STORED_0_5: case STORED_TAIL_CR: case STORED_TAIL_LF:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, STORED_COMPARABLE, ParserState.STORED_0_1);
                        break;

                    case ERROR_0_2: case ERROR_0_3: case ERROR_0_4: case ERROR_TAIL_CR: case ERROR_TAIL_LF:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, ERROR_COMPARABLE, ParserState.ERROR_0_1);
                        break;

                    case SERVER_ERROR_0_2: case SERVER_ERROR_0_3: case SERVER_ERROR_0_4: case SERVER_ERROR_0_5: case SERVER_ERROR_0_6: case SERVER_ERROR_0_7: case SERVER_ERROR_0_8: case SERVER_ERROR_0_9: case SERVER_ERROR_0_10: case SERVER_ERROR_0_11:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, SERVER_ERROR_COMPARABLE, ParserState.SERVER_ERROR_0_1);
                        break;

                    case CLIENT_ERROR_0_1: case CLIENT_ERROR_0_2: case CLIENT_ERROR_0_3: case CLIENT_ERROR_0_4: case CLIENT_ERROR_0_5: case CLIENT_ERROR_0_6: case CLIENT_ERROR_0_7: case CLIENT_ERROR_0_8: case CLIENT_ERROR_0_9: case CLIENT_ERROR_0_10: case CLIENT_ERROR_0_11:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, CLIENT_ERROR_COMPARABLE, ParserState.CLIENT_ERROR_0_1);
                        break;


                    case VALUE_0_1: case VALUE_0_2: case VALUE_0_3: case VALUE_0_4: case VALUE_0_5:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, VALUE_COMPARABLE, ParserState.VALUE_0_1);
                        break;

                    case VALUE_1: case VALUE_2:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x20) { // whitespace
                            currentState = ParserState.values()[currentState.ordinal() + 1];
                        }
                        break;
                    case VALUE_3:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x20){ // whitespace
                            currentState = ParserState.VALUE_TAIL;
                        } else if (currentValue == 0x0d) { // CR
                            currentState = ParserState.VALUE_TAIL;
                        } else {
                            dataBytesRemaining *= 10;
                            dataBytesRemaining += currentValue - 48;
                        }
                        break;
                    case VALUE_TAIL:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0a){ // LF
                            currentState = ParserState.VALUE_DATA;
                        }
                        break;
                    case VALUE_DATA:
                        int advance = Math.min(dataBytesRemaining, byteBuffer.remaining());
                        byteBuffer.position(byteBuffer.position() + advance);
                        dataBytesRemaining -= advance;

                        if (dataBytesRemaining == 0){
                            currentState = ParserState.VALUE_DATA_TAIL;
                        } else if (dataBytesRemaining < 0){
                            Log.error("[ResponseAssembler] dataBytesRemaining negative");
                        }
                        break;
                    case VALUE_DATA_TAIL:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0a){ // LF
                            valueCount++;
                            currentState = ParserState.VALUE_MORE;
                        }
                        break;

                    case VALUE_MORE:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x45){ // E
                            writingLimit = byteBuffer.position() - 1;
                            currentState = ParserState.END_0_1;
                        } else if (currentValue == 0x56){ // V
                            currentState = ParserState.VALUE_0_1;
                        } else {
                            transitionToInvalid("", currentValue);
                        }
                        break;

                    case END_0_1: case END_0_2: case END_CR: case END_LF:
                        currentValue = byteBuffer.get();
                        transitionToNextState(currentValue, END_COMPARABLE, ParserState.END_0_1);
                        break;

                    case SERVER_ERROR_MESSAGE: case CLIENT_ERROR_MESSAGE:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0d) { //CR
                            previousCharacterCR = true;
                        } else if (currentValue == 0x0a){ //LF
                            if (previousCharacterCR){
                                if (currentState == ParserState.SERVER_ERROR_MESSAGE){
                                    currentState = ParserState.SERVER_ERROR_DONE;
                                } else {
                                    currentState = ParserState.CLIENT_ERROR_DONE;
                                }
                                previousCharacterCR = false;
                            } else {
                                errorMessageStringBuilder.append(new String(new byte[]{0x0a}, StandardCharsets.US_ASCII));
                            }
                        } else {
                            byte[] append;
                            if (previousCharacterCR){
                                append = new byte[]{0x0d, currentValue};
                            } else {
                                append = new byte[]{currentValue};
                            }
                            errorMessageStringBuilder.append(new String(append, StandardCharsets.US_ASCII));
                            previousCharacterCR = false;
                        }
                        break;

                    //invalid
                    case INVALID:
                        currentValue = byteBuffer.get();
                        if (currentValue == 0x0d) { //CR
                            previousCharacterCR = true;
                        } else if (currentValue == 0x0a){ //LF
                            if (previousCharacterCR){
                                Log.error("[ResponseAssembler] Received unknown response: " + invalidCommandStringBuilder.toString());
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
                        Log.error("[ResponseAssembler] Unrecognized state in advance()");
                        break;
                }

                if (currentState == ParserState.STORED_DONE || currentState == ParserState.SERVER_ERROR_DONE
                        || currentState == ParserState.CLIENT_ERROR_DONE || currentState == ParserState.ERROR_DONE
                        || currentState == ParserState.END_DONE){
                    blocked = true;
                    if (byteBuffer.hasRemaining()){
                        Log.error("[ResponseAssembler] byteBuffer has remaining data after fully parsed response");
                    }
                    break;
                }
            }

            if (currentState == ParserState.STORED_DONE || currentState == ParserState.SERVER_ERROR_DONE
                    || currentState == ParserState.CLIENT_ERROR_DONE || currentState == ParserState.ERROR_DONE) {
                blocked = true;
                byteBuffer.flip();
            } else if (currentState == ParserState.END_DONE){
                blocked = true;
                byteBuffer.flip();
                byteBuffer.limit(writingLimit);
            }

            if (currentState == ParserState.STORED_DONE){
                ((ResponseAssemblerCompletedCallback) assemblerCompletedCallback).callback(new Result.StoredResult(this, byteBuffer, serverArrivalTime));
            }

            if (currentState == ParserState.ERROR_DONE){
                ((ResponseAssemblerCompletedCallback) assemblerCompletedCallback).callback(new Result.ErrorResult(this, byteBuffer, serverArrivalTime));
            }

            if (currentState == ParserState.SERVER_ERROR_DONE){
                ((ResponseAssemblerCompletedCallback) assemblerCompletedCallback).callback(new Result.ServerErrorResult(this, byteBuffer, errorMessageStringBuilder.toString(), serverArrivalTime));
            }

            if (currentState == ParserState.CLIENT_ERROR_DONE){
                ((ResponseAssemblerCompletedCallback) assemblerCompletedCallback).callback(new Result.ClientErrorResult(this, byteBuffer, errorMessageStringBuilder.toString(), serverArrivalTime));
            }

            if (currentState == ParserState.END_DONE){
                ((ResponseAssemblerCompletedCallback) assemblerCompletedCallback).callback(new Result.ValueResult(this, byteBuffer, serverArrivalTime, valueCount));
            }

        } catch (Exception exception) {
            Log.error("[ResponseAssembler] An exception occured in message assembly: " + exception.getMessage());
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

        // The initial S doesn't yet reveal if it's a STORED or a SERVER_ERROR response
        STORED__OR__SERVER_ERROR,

        // The initial E doesn't yet reveal if it's a ERROR or an END response
        ERROR__OR__END,

        // set response
        STORED_0_1, STORED_0_2, STORED_0_3, STORED_0_4, STORED_0_5, STORED_TAIL_CR, STORED_TAIL_LF, STORED_DONE,

        // get responses
        VALUE_0_1, VALUE_0_2, VALUE_0_3, VALUE_0_4, VALUE_0_5,
        VALUE_1, VALUE_2, VALUE_3, VALUE_TAIL,
        VALUE_DATA, VALUE_DATA_TAIL,
        //VALUE_MORE checks if END\CR\LF follows (no more values) or another value block (then transitioning to VALUE_0_2)
        VALUE_MORE,

        END_0_1, END_0_2, END_CR, END_LF, END_DONE,

        // errors
        ERROR_0_1, ERROR_0_2, ERROR_0_3, ERROR_0_4, ERROR_TAIL_CR, ERROR_TAIL_LF, ERROR_DONE,
        SERVER_ERROR_0_1, SERVER_ERROR_0_2, SERVER_ERROR_0_3, SERVER_ERROR_0_4, SERVER_ERROR_0_5, SERVER_ERROR_0_6, SERVER_ERROR_0_7, SERVER_ERROR_0_8, SERVER_ERROR_0_9, SERVER_ERROR_0_10, SERVER_ERROR_0_11, SERVER_ERROR_MESSAGE, SERVER_ERROR_DONE,
        CLIENT_ERROR_0_1, CLIENT_ERROR_0_2, CLIENT_ERROR_0_3, CLIENT_ERROR_0_4, CLIENT_ERROR_0_5, CLIENT_ERROR_0_6, CLIENT_ERROR_0_7, CLIENT_ERROR_0_8, CLIENT_ERROR_0_9, CLIENT_ERROR_0_10, CLIENT_ERROR_0_11, CLIENT_ERROR_MESSAGE, CLIENT_ERROR_DONE
    }

    /*
     Writes everything from current position to limit to the server.
     After, it rewinds its position to what it was before writing.
      */
    public void setLimitToEnd(){
        if (currentState != ParserState.END_DONE){
            Log.warn("[ResponseAssembler] Non-Value response limit set to end for client writing");
        }
        byteBuffer.limit(writingLimit + 5); //END\CR\LF
    }

    // This interface is used by Parser to outsource the processing of created jobs
    public interface ResponseAssemblerCompletedCallback extends AssemblerCompletedCallback<Result>{
        // No additional methods, only callback<Result> from super
    }

    void setServerArrivalTime(long serverArrivalTime){
        this.serverArrivalTime = serverArrivalTime;

    }
}