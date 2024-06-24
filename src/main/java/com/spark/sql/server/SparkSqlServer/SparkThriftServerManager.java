package com.spark.sql.server.SparkSqlServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.JSONArray;
import org.json.JSONObject;

public class SparkThriftServerManager {
    private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/default";
    private final Connection connection;
    
    public SparkThriftServerManager() throws Exception {
        // Establish connection
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection(CONNECTION_URL, "", "");
    }

    public String submitQuery(final String query) throws Exception {
        Statement statement = null;
        System.out.println("Start of query command " + query);

        try {
            // Load Hive JDBC drive
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            return convertResultSetToJson(resultSet);
        } finally {
            // Close resources
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void execute(final String command) throws Exception {
        Statement statement = null;
        System.out.println("Start of execute command " + command);

        try {
            // Load Hive JDBC drive
            statement = connection.createStatement();
            statement.execute(command);
        } finally {
            // Close resources
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
}
