package com.test.xtlmaven;

import java.io.IOException;

import org.apache.spark.sql.*;

/**
 * Hello world!
 *
 */
public class App {

    /**
     * Extract SparkSQL data from CSV file
     * 
     * @param executionMode execution mode for Spark cluster (local, master, etc.)
     * @param inputFilePath input file path for data source
     * @param query         query to run on input data
     * @throws IOException if error, throw IOException
     *                     CONFIGURATION INFORMATION REQUIRED:
     *                     - execution mode for Spark SQL
     *                     - input filepath
     *                     - query to run
     *                     - SparkSQL system designation
     */
    static public void extractSparkSql(String executionMode, String inputFilePath, String query) throws IOException {
        // connect to spark sql url, run query on it
        SparkSession spark = SparkSession.builder().appName("XTL Spark SQL app").master(executionMode).getOrCreate();
        // System.out.println(spark);
        Dataset<Row> df = spark.read().option("header", "true").csv(inputFilePath);
        df.createOrReplaceTempView("XTLInput");

        // run initial query on file
        Dataset<Row> sqlQuery = spark.sql(query);

        // save results in CSV directory - use *.csv to get file with results
        sqlQuery.write().csv("data/sparksqlExtracted.csv");
    }

    public static void main(String[] args) throws IOException {
        // Call extract Spark SQL function
        extractSparkSql("local", "resources/SparkSQLOutputData.csv", "SELECT service FROM XTLInput");
    }
}
