package app.SparkSQL;

import org.apache.spark.sql.*;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.FileUtils;
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
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load(inputFilePath);
        df.createOrReplaceTempView("XTLInput");

        // run initial query on file
        Dataset<Row> sqlQuery = spark.sql(query);

        // save results in CSV directory - use *.csv to get file with results
        sqlQuery.write().csv("data/sparksqlExtracted.csv");

        // File csvDirectory = new File("data/sparksqlExtracted.csv");
        // FileFilter regex = new RegexFileFilter("*.csv");
        // File[] csvFile = csvDirectory.listFiles(regex);
        // System.out.println("FINDING FILE");
        // System.out.println(csvFile);
        try {
            // HDFSUtils.copyFromLocal(csvFile[0].getName(), Constants.WORKING_DIR, "addr",
            // 8080);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
