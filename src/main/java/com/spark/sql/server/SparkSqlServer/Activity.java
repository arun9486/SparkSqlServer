package com.spark.sql.server.SparkSqlServer;

public class Activity {
    private MapStore mapStore;
    
    public Activity(final MapStore mapStore) {
        this.mapStore = mapStore;
    }

    public String CreateJob(final String command) {
        Job job = new Job(command);
        return this.mapStore.putJob(job);
    }
    
    public Job GetJob(final String commandID) {
        return this.mapStore.getJob(commandID);
    }
}
