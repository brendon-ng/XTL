package app.SparkSQL;

import org.apache.spark.sql.*;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.filefilter.RegexFileFilter;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

public class SparkSQLImporter extends Connector {
    private String executionMode;
    private String inputFilepath;
    private String query;

    public SparkSQLImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.executionMode = (String) config.get("executionMode");
        this.inputFilepath = (String) config.get("inputFilepath");
        this.query = (String) config.get("query");
    }

    public void execute() {
        // connect to spark sql url, run query on it
        SparkSession spark = SparkSession.builder().appName("XTL Spark SQL app").master(executionMode).getOrCreate();
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load(inputFilepath);
        df.createOrReplaceTempView("XTLInput");

        // run initial query on file
        Dataset<Row> sqlQuery = spark.sql(query);

        // save results in CSV directory - use *.csv to get file with results
        sqlQuery.write().csv("data/sparksqlExtracted.csv");

        File csvDirectory = new File("data/sparksqlExtracted.csv");
        FileFilter regex = new RegexFileFilter(".*.csv");
        File[] csvFile = csvDirectory.listFiles(regex);

        try {
            for (int i = 0; i < csvFile.length; i++) {
                String formattedPath = String.format("data/sparksqlExtracted.csv/%s", csvFile[i].getName());
                HDFSUtils.copyFromLocal(formattedPath, Constants.WORKING_DIR, Constants.HDFS_WORKING_ADDR,
                        Constants.HDFS_WORKING_PORT);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        spark.close();
    }
}
