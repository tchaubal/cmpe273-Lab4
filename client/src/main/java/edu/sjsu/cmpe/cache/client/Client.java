package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Unirest;

import java.util.*;
import java.lang.*;
import java.io.*;

public class Client {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CRDTClient crdtClient = new CRDTClient();
        
        
        //Step 1.1: HTTP Put call to store Key: Value as "a":"1"
        boolean result = crdtClient.put(1, "a");
        //Result is : a a a
        System.out.println("Result after putting key value (1=>a) " + result);
        // Step 1.2:  Sleeping for 30 secs so that there is enough time to stop the server A
        System.out.println("Sleeping for 30 secs so that there is enough time to stop the server A");
        Thread.sleep(30000);
        System.out.println("Step 1: put(1 => a) and 30 sec sleep complete. Server A should be down.");
        // Second HTTP PUT call to update key 1 value to “b”. (Then, sleep again for another ~30 seconds while bringing the server A back)
        // Result: null b b
        //Second step is to update key 1 with value 'b'.
        // Then sleep for another 30 sec to bring the server a up.
        System.out.println("Step 2: Updating key 1 with value b");
        crdtClient.put(1, "b");
        System.out.println("Sleeping for 30 sec so that there is time to up the server a");
        Thread.sleep(30000);
        System.out.println("Step 2: put(1 => b) and 30 sec sleep complete");
        
        // Final HTTP GET call to retrieve key “1” value.
        // Result b b b
        System.out.println("Repairing server a to get updated value in all servers.");
        String value = crdtClient.get(1);
        System.out.println("Step 3: get(1) => " + value);
        System.out.println("Exiting Client...");
        Unirest.shutdown();
    }
    
}
