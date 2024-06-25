package com.spark.sql.server.SparkSqlServer;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MapStore {
    private ConcurrentHashMap<String, Job> store;
    private ConcurrentLinkedQueue<Job> queue = new ConcurrentLinkedQueue<>();


    public MapStore() {
        this.store = new ConcurrentHashMap();
    }
    
    public String putJob(final Job job) {
        final String commandId = UUID.randomUUID().toString();
        
        if (!store.contains(commandId)) {
	        job.setCommandId(commandId);
	        job.setState("PENDING");
        }

        this.store.put(commandId, job);
        queue.add(job);
        
        return commandId;
    }
    
    public void updateJob(final Job job) {
        final String commandId = job.getCommandId();

        if (commandId == null) {
            throw new RuntimeException("No commandId found for update");
        }
        
        this.store.put(commandId, job);
    }
    
    public Job getJob(final String commandId) {
        return store.get(commandId);
    }
    
    public Job getNextItem() {
        if (!queue.isEmpty()) {
            return queue.poll();
        }
        
        return null;
    }
}
