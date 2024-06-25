package com.spark.sql.server.SparkSqlServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.spark.sql.server.utils.Utils;

public class Reader {
    private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/default";
    private MapStore mapStore;
    private boolean exitReader = false;
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    
    public Reader(final MapStore mapStore) {
        this.mapStore = mapStore;
    }
    
    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Hive JDBC Driver not found", e);
        }
    }
    
    public void startReader() throws Exception {
           // Submit the task
        executorService.submit(() -> {
            try {
                process();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }); 
    }

    public void process() throws Exception {
        Connection connection = null;
        Statement statement = null;
        // Load Hive JDBC drive
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        // Establish connection
        connection = DriverManager.getConnection(CONNECTION_URL, "", "");
        statement = connection.createStatement();
        
        System.out.println("Starting reader");
        while (exitReader == false) {
            try {
	            Job job = this.mapStore.getNextItem();
                
                if (job != null) {
                    System.out.println("Reader: Found command " + job.getCommand());
		            String command = job.getCommand();
		            
		            if (command.contains("INSERT") || command.contains("CREATE")) {
		                statement.execute(command);
		            } else if (command.contains("SELECT")){
	                    ResultSet resultSet = statement.executeQuery(command);
	                    String result = convertResultSetToJson(resultSet);
	                    job.setResult(result);
	                }

                    job.setState("COMPLETE");
                    System.out.println("Reader: Execution of command completed");
	                this.mapStore.updateJob(job);
                }
            } catch (Exception e) {
                System.out.println("Unable to run command " + e.getMessage());
            }

            Utils.delay(500);
        }
        
        // Close resources
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String convertResultSetToJson(ResultSet resultSet) throws SQLException {
        JSONArray jsonArray = new JSONArray();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = resultSet.getObject(i);
                jsonObject.put(columnName, columnValue);
            }
            jsonArray.put(jsonObject);
        }

        return jsonArray.toString();
    }

    public void exitReader() {
        this.exitReader = true;
        
        // Shutdown the executor
        executorService.shutdown();
    }
}
