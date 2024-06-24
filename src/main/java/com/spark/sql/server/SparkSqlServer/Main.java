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
        //clusterManager.startCluster();
        clusterManager.startThriftServer();
    }
    
    private void cleanCluster() {
        clusterManager.stopCluster();
    }
    
    private void test() throws Exception {
        System.out.println("Starting test");
        Main main = new Main();
        main.initClusterWithThriftServer();
        
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
        main.test();
        // main.initClusterWithThriftServer();

        // SparkThriftServerManager sparkThriftServerManager = new SparkThriftServerManager();
        // Reader reader = new Reader(sparkThriftServerManager);
        // reader.startReader();
        
        // Activity activity = new Activity();

        // while (true) {
        //     Scanner scanner = new Scanner(System.in);

        //     System.out.println("Enter the command");
        //     String command = scanner.nextLine();
        //     System.out.println("Command received: " + command);

        //     if (command.contains("quit")) {
        //         System.out.println("Exit command received...");
        //         reader.exitReader();
        //         Utils.delay(2000);
        //         break;
        //     } else if (command.contains("get")) {
        //         System.out.println("Enter the commandId");
        //         String commandId = scanner.nextLine();
        //         System.out.println("CommandID received: " + commandId);
                
        //         // System.out.println(activity.GetJob(commandId));
        //         System.out.println("Get command completed");
        //     } else {
        //         System.out.println("Enter the command type");
        //         String commandType = scanner.nextLine();
        //         System.out.println("Command type received: " + commandType);

        //         // activity.CreateJob(command, commandType);
        //         System.out.println("Modify command completed");
        //     }
        // }
    }
}
