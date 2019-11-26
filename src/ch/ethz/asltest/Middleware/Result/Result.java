package ch.ethz.asltest.Middleware.Result;

import ch.ethz.asltest.Middleware.Assembler.ResponseAssembler;

import java.nio.ByteBuffer;

/*
    The abstract Result class represents the answer of a server to a query. It is extended by concrete
    responses like StoredResult (returned on successful set operation) or ValueResult. The response can be forwarded
    to clients using the bound ResponseAssembler (Result objects are created upon completing parsing therein).
 */

public abstract class Result{

    protected ResponseAssembler responseAssembler;

    protected ByteBuffer byteBuffer;

    protected long serverArrivalTime;

    Result(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, long serverArrivalTime){
        this.responseAssembler = responseAssembler;
        this.byteBuffer = byteBuffer;
        this.serverArrivalTime = serverArrivalTime;
    }


    public ResponseAssembler getResponseAssembler(){
        return responseAssembler;
    }

    public long getServerArrivalTime(){
        return serverArrivalTime;
    }


    public static class StoredResult extends Result{
        public StoredResult(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, long serverArrivalTime){
            super(responseAssembler, byteBuffer, serverArrivalTime);
        }
    }

    public static class ErrorResult extends Result{
        public ErrorResult(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, long serverArrivalTime){
            super(responseAssembler, byteBuffer, serverArrivalTime);
        }

    }

    public static class ClientErrorResult extends Result{

        public String errorMessage;

        public ClientErrorResult(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, String errorMessage, long serverArrivalTime){
            super(responseAssembler, byteBuffer, serverArrivalTime);
            this.errorMessage = errorMessage;
        }
    }

    public static class ServerErrorResult extends Result{

        public String errorMessage;

        public ServerErrorResult(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, String errorMessage, long serverArrivalTime){
            super(responseAssembler, byteBuffer, serverArrivalTime);
            this.errorMessage = errorMessage;
        }
    }

    public static class ValueResult extends Result{

        public final int valueCount;

        public ValueResult(ResponseAssembler responseAssembler, ByteBuffer byteBuffer, long serverArrivalTime, int valueCount){
            super(responseAssembler, byteBuffer, serverArrivalTime);
            this.valueCount = valueCount;
        }

        public void setLimitToEnd(){
            responseAssembler.setLimitToEnd();
        }
    }
}