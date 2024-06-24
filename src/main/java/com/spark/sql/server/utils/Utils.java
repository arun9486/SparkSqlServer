package com.spark.sql.server.utils;

public class Utils {
    
    public static void delay(final long ms) {
        try {
            Thread.sleep(ms);
        } catch(Exception e) {
            System.out.println("There was an error in setting sleep");;
        }
    }
}
