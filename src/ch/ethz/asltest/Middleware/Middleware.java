package ch.ethz.asltest.Middleware;

import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Environment.Server;
import ch.ethz.asltest.Middleware.Global.Configuration;
import ch.ethz.asltest.Middleware.Global.Global;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.JobQueue;
import ch.ethz.asltest.Middleware.Log.Log;
import ch.ethz.asltest.Middleware.Log.Statistics;
import ch.ethz.asltest.Middleware.Threading.NetThread;
import ch.ethz.asltest.Middleware.Threading.ShutdownThread;
import ch.ethz.asltest.Middleware.Threading.WorkerPool;

import java.util.*;
import java.util.concurrent.TimeUnit;

/*
    Main class initializing the middleware by setting Global values and Configurations and parsing Parameters.
    The Environment will also be initialized here.
 */

public class Middleware{

    public Middleware(String ip, int port, List<String> mcAddresses, int numThreadsPTP, boolean readSharded){

        Log.initialize();

        Global.launchTime = (System.currentTimeMillis() / 1000L);

        // Set config
        Configuration.initialize();
        Configuration.setValues(ip, port, mcAddresses, numThreadsPTP, readSharded);

        // Parse parameters
        Parameters.initialize();
        Parameters.parse();

        // Shutdown hook
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new ShutdownThread(Thread.currentThread()));

        // Populate the environment with servers
        Environment.initialize();
        for (String address : mcAddresses){
            String[] splitAddress = address.split(":");
            Environment.getServerList().add(new Server(splitAddress[0], Integer.parseInt(splitAddress[1])));
        }

        JobQueue.initialize();

        WorkerPool.initialize();

        NetThread.initialize();

    }


    public void run(){
        Log.info("Starting middleware version " + Parameters.getInteger("version_major") + "." + Parameters.getInteger("version_minor") + "." + Parameters.getInteger("version_revision"));
        NetThread.getInstance().run();
    }


}