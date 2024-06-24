package com.spark.sql.server.SparkSqlServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SparkThriftServerExampleOld {
    private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/default";

    public static void main(String[] args) throws Exception {
        Connection connection = null;
        Statement statement = null;
        
        System.out.println("Start of application");

        try {
            // Load Hive JDBC drive
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            // Establish connection
            connection = DriverManager.getConnection(CONNECTION_URL, "", "");
            statement = connection.createStatement();
            
            System.out.println("Connection established");

            // Create table
            String createTableSQL = "CREATE TABLE IF NOT EXISTS example_table (id INT, name STRING)";
            statement.execute(createTableSQL);

            // Insert rows
            String insertRow1SQL = "INSERT INTO example_table VALUES (1, 'Alice')";
            statement.execute(insertRow1SQL);

            String insertRow2SQL = "INSERT INTO example_table VALUES (2, 'Bob')";
            statement.execute(insertRow2SQL);

            // Query the table
            String querySQL = "SELECT * FROM example_table";
            ResultSet resultSet = statement.executeQuery(querySQL);

            // Print the results
            while (resultSet.next()) {
                System.out.println("Inside result set while");
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println("ID: " + id + ", Name: " + name);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
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
    }
}