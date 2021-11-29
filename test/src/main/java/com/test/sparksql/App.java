package com.test.sparksql;

import java.io.IOException;

import org.apache.spark.sql.*;

/**
 * Hello world!
 *
 */
public class App {
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
        extractSparkSql("local", "resources/SparkSQLOutputData.csv", "SELECT service FROM XTLInput");
    }
}
