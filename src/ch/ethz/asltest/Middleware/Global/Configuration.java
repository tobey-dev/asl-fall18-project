package ch.ethz.asltest.Middleware.Global;

/*
    This class contains the MW configuraion set by the command line arguments.
 */

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Configuration{

    private static Configuration ourInstance;

    public static void initialize(){
        ourInstance = new Configuration();
    }

    private Configuration(){

    }

    private static boolean valuesSet = false;


    private static String ip;
    private static int port;
    private static List<String> mcAddresses;
    private static int threadCount;
    private static boolean readSharded;

    public static String getIp(){
        if (!valuesSet){
            throw new RuntimeException("Trying to access non-set configuration");
        }
        return new String(ip);
    }

    public static int getPort(){
        if (!valuesSet){
            throw new RuntimeException("Trying to access non-set configuration");
        }
        return port;
    }


    public static List<String> getMcAddresses(){
        if (!valuesSet){
            throw new RuntimeException("Trying to access non-set configuration");
        }
        return mcAddresses;
    }


    public static int getThreadCount(){
        if (!valuesSet){
            throw new RuntimeException("Trying to access non-set configuration");
        }
        return threadCount;
    }

    public static boolean getReadSharded(){
        if (!valuesSet){
            throw new RuntimeException("Trying to access non-set configuration");
        }
        return readSharded;
    }

    public static void setValues(String ip, int port, List<String> mcAddresses, int threadCount, boolean readSharded){
        Configuration.ip = ip;
        Configuration.port = port;
        Configuration.mcAddresses = mcAddresses;
        Configuration.threadCount = threadCount;
        Configuration.readSharded = readSharded;
        valuesSet = true;
    }

}