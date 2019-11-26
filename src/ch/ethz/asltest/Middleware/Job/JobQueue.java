package ch.ethz.asltest.Middleware.Job;

import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Log.Log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
    The JobQueue contains all jobs received and parsed, but not yet processed. Gets polled by WorkerThreads.
    This class is a wrapper over a BlockingQueue of type Job, exposing a minimal interface.
 */

public class JobQueue {

    private static JobQueue ourInstance;
    public static JobQueue getInstance() {
        return ourInstance;
    }

    private BlockingQueue<Job> blockingQueue;

    public static void initialize(){
        ourInstance = new JobQueue();
    }

    private JobQueue(){
        blockingQueue = new ArrayBlockingQueue<>(Parameters.getInteger("queue_capacity"), Parameters.getBoolean("queue_fifo"));
    }

    public void put(Job job){
        try {
            blockingQueue.put(job);
        } catch (InterruptedException interruptedException){
            Log.warn("Interruption exception encountered in Queue draw method: " + interruptedException.getMessage());
        }
    }

    public Job poll(long timeout, TimeUnit unit) throws InterruptedException{
        return blockingQueue.poll(timeout, unit);
    }

    public int getQueueSize(){
        return blockingQueue.size();
    }





}
