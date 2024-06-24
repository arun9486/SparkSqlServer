package com.spark.sql.server.SparkSqlServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.spark.sql.server.utils.Utils;

public class ClusterManager {
    private String scriptPath = "/home/arun-linux/projects/github/docker-spark/playground/bb.sh";
    private String masterContainerName = "playground-spark-master";

    private String startClusterTargetString = "Bound HistoryServer to 0.0.0.0, and started at"; // Replace with the actual output to wait for
    private String startClusterScriptArgument = "restart"; 
    
    private String stopClusterTargetString = "Total reclaimed space";
    private String stopClusterScriptArgument = "stop";

    private String startThriftServerCommand =  "/home/arun-linux/projects/spark/sbin/start-thriftserver.sh --master yarn --deploy-mode client --conf spark.sql.warehouse.dir=/tmp";
    private String startThriftServerTargetString = "LiveListenerBus.queue.shared.listenerProcessingTime";

    public void startCluster() {
        runCommandAndWait(startClusterTargetString, "sh", scriptPath, startClusterScriptArgument);
    }
    
    public void stopCluster() {
        runCommandAndWait(stopClusterTargetString, "sh", scriptPath, stopClusterScriptArgument);
    }
    
    public void startThriftServer() {
        System.out.println("Starting of thrift server");
        runCommandAndWait(startThriftServerTargetString, "docker", "exec", masterContainerName, "sh", "-c", startThriftServerCommand);
    }
    
    private void runCommandAndWait(final String output, String... startArguments) {
        ProcessBuilder processBuilder = new ProcessBuilder(startArguments);
        processBuilder.redirectErrorStream(true);

        try {
            Process process = processBuilder.start();
            System.out.println("Command sent");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;

            while ((line = reader.readLine()) != null) {
                System.out.println(line); // Print the console output

                if (line.contains(output)) {
                    Utils.delay(2000);
                    System.out.println("Desired output found, exiting");
                    return;
                }
            }

            process.waitFor(); // Wait for the process to complete
            reader.close();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public boolean isClusterRunning() {
        return false;
    }
}
