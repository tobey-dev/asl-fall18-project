package ch.ethz.asltest;

import ch.ethz.asltest.Middleware.Middleware;

import java.util.*;

    /*
        This class acts as a wrapper for Middleware.
     */


public class MyMiddleware{

    protected Middleware middleware;


    public MyMiddleware(String myIp, int myPort, List<String> mcAddresses, int numThreadsPTP, boolean readSharded){
        middleware = new Middleware(myIp, myPort, mcAddresses, numThreadsPTP, readSharded);
    }


    public void run(){
        middleware.run();
    }

}