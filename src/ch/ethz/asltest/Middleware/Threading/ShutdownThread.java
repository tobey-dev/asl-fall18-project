package ch.ethz.asltest.Middleware.Threading;

import ch.ethz.asltest.Middleware.Global.Global;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Log.Log;

import java.util.List;
import java.util.concurrent.TimeUnit;

/*
    The ShutdownThread will be started once the shutdown hook is activated. It will instruct all other threads
    to write out their Statistics and then stop. It holds a panic mechanism to allow returning an error code
    which is to overwrite the normal error code in case an exception occurs during shutdown.
 */

public class ShutdownThread extends Thread{

    private Thread mainThread;
    private static int returncode = 0;

    public ShutdownThread(Thread mainThread){
        super();
        setName("shutdown");
        this.mainThread = mainThread;
    }

    public static void panic(int returncode){
        Log.fatal("PANIC SHUTDOWN CODE " + returncode);
        System.exit(returncode);
    }

    public static void setReturncode(int returncode){
        if ((ShutdownThread.returncode != 0) && (returncode == 0)){
            // Don't allow overwriting an error with a non-error
            return;
        }
        ShutdownThread.returncode = returncode;
    }

    @Override
    public void run() {
        super.run();
        Log.info("[ShutdownThread] Shutting down started");

        // Causes NetThread and WorkerThreads to shut down
        Global.isShuttingDown = true;

        // Shut down WorkerPool
        WorkerPool workerPool = WorkerPool.getInstance();
        if (workerPool != null) {
            workerPool.shutdown();
        }


        // Shut down NetThread
        try {
            mainThread.join(Parameters.getLong("netthread_shutdown_timeout_ms"));
            if (mainThread.isAlive()){
                mainThread.interrupt();
                Log.info("[ShutdownThread] Main thread interrupted");
                mainThread.join();
            }
        } catch (InterruptedException interruptedException){
            Log.warn("[ShutdownThread] Shutting down main thread was interrupted");
        }
        Log.info("[ShutdownThread] Shutdown complete");

        Log.shutdown();

        if (returncode != 0){
            Runtime.getRuntime().halt(returncode);
        }
    }
}
