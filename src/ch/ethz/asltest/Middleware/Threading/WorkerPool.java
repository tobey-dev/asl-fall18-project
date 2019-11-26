package ch.ethz.asltest.Middleware.Threading;

import ch.ethz.asltest.Middleware.Global.Configuration;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Log.Log;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
    The WorkerPool class holds a set of WorkerThreads, which are all instantiated in this class. On shutdown this class
    orchestrates the shutdown of all its WorkerThreads.
 */

public class WorkerPool {

    private static WorkerPool ourInstance;
    public static WorkerPool getInstance() {
        return ourInstance;
    }

    public static void initialize() {
        ourInstance = new WorkerPool(Configuration.getThreadCount());
    }

    Set<WorkerThread> workerThreadSet;

    private WorkerPool(int threadCount){
        Log.info("[WorkerPool] Instantiating WorkerPool with " + threadCount + " thread(s)");
        workerThreadSet = new HashSet<>();
        WorkerThread currentWorkerThread;
        for (int i = 0; i < threadCount; i++){
            currentWorkerThread = new WorkerThread();
            workerThreadSet.add(currentWorkerThread);
            currentWorkerThread.start();
        }
        Log.info("[WorkerPool] WorkerThreads created successfully");
    }


    public void shutdown(){
        Log.info("[WorkerPool] Shutting down WorkerPool");
        for (WorkerThread workerThread : workerThreadSet){
            try {
                workerThread.join(Parameters.getLong("worker_pool_join_timeout_ms"));
                if (workerThread.isAlive()) {
                    workerThread.interrupt();
                    Log.info("[WorkerPool] WorkerThread \"" + workerThread.getName() + "\" has been interrupted.");
                    workerThread.join();
                }
            } catch (InterruptedException interruptedException){
                Log.warn("[WorkerPool] WorkerThread \"" + workerThread.getName() + "\" could not be joined: " + interruptedException.getMessage());
            }
        }
        Log.info("[WorkerPool] All WorkerThreads joined, WorkerPool shutdown done");

    }


}
