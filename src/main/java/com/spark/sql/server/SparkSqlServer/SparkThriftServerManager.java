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

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Hive JDBC Driver not found", e);
        }
    }

    public String submitQuery(final String query) throws Exception {
        try (Connection connection = DriverManager.getConnection(CONNECTION_URL, "", "");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            System.out.println("Start of query command " + query);
            return convertResultSetToJson(resultSet);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Failed to execute query: " + query, e);
        }
    }

    public void execute(final String command) throws Exception {
        try (Connection connection = DriverManager.getConnection(CONNECTION_URL, "", "");
             Statement statement = connection.createStatement()) {

            System.out.println("Start of execute command " + command);
            statement.execute(command);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Failed to execute command: " + command, e);
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
