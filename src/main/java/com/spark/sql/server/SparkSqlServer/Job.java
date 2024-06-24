package com.spark.sql.server.SparkSqlServer;

public class Job {
    private String command;
    private String commandType;
    private String commandId;
    private String state;
    private String result;
    
    public Job(final String command, final String commandType) {
        this.command = command;
        this.commandType = commandType;
    }

    public Job(final String command, final String commandType, final String commandId) {
        this.command = command;
        this.commandType = commandType;
        this.commandId = commandId;
    }
    
    public void setCommandId(final String commandId) {
        this.commandId = commandId;
    }
    
    public void setState(final String state) {
        this.state = state;
    }
    
    public void setResult(final String result) {
        this.result = result;
    }
    
    public String getCommand() {
        return this.command;
    }
    
    public String getCommandType() {
        return this.commandType;
    }
    
    public String getCommandId() {
        return this.commandId;
    }
    
    public String getState() {
        return state;
    }
}
