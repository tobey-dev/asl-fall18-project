package ch.ethz.asltest.Middleware.Assembler;

import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Log.Log;

import javax.xml.ws.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*
    This class defines an abstract Assembler. An Assembler is to be fed data (e.g. by associating it with
    a SocketChannel), which then parses the data and fires a callback function with a constructed appropriate
    object representing the parsed message. The Assembler can be advanced as many bytes as are available
    at a time (also more bytes than remain in one message). The Assembler allows parsing and forwarding
    messages without intermediate copies (excluding from and to SocketChannel buffers)
 */

public abstract class Assembler {


    AssemblerCompletedCallback assemblerCompletedCallback;

    ByteBuffer byteBuffer;
    boolean blocked;


    public Assembler(AssemblerCompletedCallback assemblerCompletedCallback){
        this.assemblerCompletedCallback = assemblerCompletedCallback;
        byteBuffer = ByteBuffer.allocate(Parameters.getInteger("assembler_meta_rcv_buffer_size"));
    }

    public static class AssemblerBlockedException extends Exception {
        public AssemblerBlockedException(String message){
            super(message);
        }
    }

    /*
    This method clears the buffer and resets the parser state.
    Needs to be called after the buffer has been used and its content can be discarded.
     */
    public void release(){
        byteBuffer.clear();
        blocked = false;
    }

    public boolean isBlocked(){
        return blocked;
    }

    /*
     advance is called after new data in the bytebuffer becomes available.
     it needs to consume all new data available (until bytebuffer.hasRemaining is false)
     advance may call the callback once a certain event is triggered.
      */

    abstract void advance();

    /*
     This interface allows registering a callback object to be notified once a message
     has been completed.
      */
    public interface AssemblerCompletedCallback<T>{
        void callback(T t);
    }

    /*
        Finds the index of the n-th occurrence of value.
        Starts from the current position and goes up to limit.
        Returns -1 if no occurrence was found.
     */
    public final int getNthIndex(byte value, int n){
        int position = -1;
        int counter = 0;

        if (n == 0){
            return position;
        }

        for (int i = byteBuffer.position(); i < byteBuffer.limit(); i++){
            if (byteBuffer.get(i) == value){
                counter++;
                if (counter == n){
                    position = i;
                    break;
                }
            }
        }

        return position;
    }

    /*
    Finds the total number of occurrences of value inside the buffer.
    Starts from the current position and goes up to limit.
    Returns 0 if byteBuffer is zero.
    */
    public final int getOccurrenceCount(byte value){
        int counter = 0;

        for (int i = byteBuffer.position(); i < byteBuffer.limit(); i++){
            if (byteBuffer.get(i) == value){
                counter++;
            }
        }

        return counter;
    }
}
