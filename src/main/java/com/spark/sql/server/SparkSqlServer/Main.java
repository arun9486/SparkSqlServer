package com.spark.sql.server.SparkSqlServer;

import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileContext.Util;

import com.spark.sql.server.utils.Utils;

public class Main {
    private ClusterManager clusterManager = new ClusterManager();
    
    public Main() throws Exception {
        clusterManager = new ClusterManager();
    }

    private void initClusterWithThriftServer() {
        clusterManager.startCluster();
        clusterManager.startThriftServer();
    }
    
    private void cleanCluster() {
        clusterManager.stopCluster();
    }
    
    private void test() throws Exception {
        System.out.println("Starting test");
        Main main = new Main();
        //main.initClusterWithThriftServer();
        
        SparkThriftServerManager sparkThriftServerManager = new SparkThriftServerManager();
        // create table
        sparkThriftServerManager.execute("CREATE TABLE IF NOT EXISTS example_table (id INT, name STRING)");

        // insert rows
        sparkThriftServerManager.execute("INSERT INTO example_table VALUES (1, 'Alice')");
        sparkThriftServerManager.execute("INSERT INTO example_table VALUES (2, 'Bob')");
        
        // query table
        String result = sparkThriftServerManager.submitQuery("SELECT * FROM example_table");
        System.out.println(result);
        
        // stop cluster
        // main.cleanCluster();
    }
    
    private void test2() throws Exception {
        SparkThriftServerManager sparkThriftServerManager = new SparkThriftServerManager(); 
    }

    // this is just for testing
    public static void main(String [] args) throws Exception {
        Main main = new Main();
        main.initClusterWithThriftServer();

        MapStore mapStore = new MapStore();
        Reader reader = new Reader(mapStore);
        reader.startReader();
        
        Activity activity = new Activity(mapStore);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter api");
            String api = scanner.nextLine();
            System.out.println("API received: " + api);

            if (api.contains("quit")) {
                System.out.println("Exit command received...");
                reader.exitReader();
                Utils.delay(2000);
                break;
            } else if (api.contains("get")) {
                System.out.println("Enter the commandId");
                String commandId = scanner.nextLine();
                System.out.println("CommandID received: " + commandId);
                
                Job job = activity.GetJob(commandId);
                System.out.println(job.getCommandId());
                System.out.println(job.getCommand());
                System.out.println(job.getState());

                if (job.getCommand().contains("SELECT")) {
                    System.out.println(job.getResult());
                }
            } else if (api.contains("put")) {
                System.out.println("Enter the command");
                String command = scanner.nextLine();
                String commandId = activity.CreateJob(command);
                System.out.println("Modify command completed " + commandId);
            } else {
                System.out.println("Unaknown command");
            }
        }
        
        scanner.close();
    }
}
