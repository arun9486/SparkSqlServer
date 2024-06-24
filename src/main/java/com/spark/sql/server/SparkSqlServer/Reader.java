package com.spark.sql.server.SparkSqlServer;

import com.spark.sql.server.utils.Utils;

public class Reader {
    private MapStore mapStore = new MapStore();
    private boolean exitReader = false;
    private SparkThriftServerManager sparkThriftServerManager;
    
    public Reader(SparkThriftServerManager sparkThriftServerManager) throws Exception {
        this.sparkThriftServerManager = sparkThriftServerManager;
    }
    
    public void startReader() {
        while (exitReader == false) {
            try {
	            Job job = this.mapStore.getNextItem();
	            String commandType = job.getCommandType();
	            String command = job.getCommand();
	            
	            if (commandType == "MODIFY") {
	                this.sparkThriftServerManager.execute(command);
                    job.setState("COMPLETE");
	            } else {
                    String result = this.sparkThriftServerManager.submitQuery(command);
                    job.setResult(result);
                    job.setState("COMPLETE");
                }
                
                this.mapStore.updateJob(job);
            } catch (Exception e) {
                System.out.println("Unable to run command");
            }

            Utils.delay(500);
        }
    }

    public void exitReader() {
        this.exitReader = true;
    }
}
