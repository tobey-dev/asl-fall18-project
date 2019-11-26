package ch.ethz.asltest.Middleware.Environment;

import java.util.ArrayList;

/*
    The Environment class contains all current client and server connections.
 */

public class Environment {

    protected static ArrayList<Client> clientList;
    protected static ArrayList<Server> serverList;

    private static Environment ourInstance;

    public static void initialize(){
        ourInstance = new Environment();
    }

    private Environment(){
        clientList = new ArrayList<>();
        serverList = new ArrayList<>();
    }

    public static ArrayList<Client> getClientList() {
        return clientList;
    }

    public static ArrayList<Server> getServerList() {
        return serverList;
    }

    public static class Entity{
        // Empty class used to represent both Clients and Servers
    }
}
