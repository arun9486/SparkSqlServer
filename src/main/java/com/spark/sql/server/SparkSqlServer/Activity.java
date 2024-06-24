package com.spark.sql.server.SparkSqlServer;

public class Activity {
    private ClusterManager clusterManager = new ClusterManager();
    private MapStore mapStore = new MapStore();
    

    public String CreateJob(final String command, final String commandType) {
        Job job = new Job(command, commandType);
        return this.mapStore.putJob(job);
    }
    
    public Job GetJob(final String commandID) {
        return this.mapStore.getJob(commandID);
    }
}
