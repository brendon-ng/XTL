package app.SparkSQL;

import org.apache.spark.SparkContext;
import org.json.simple.JSONObject;

import app.Connector;

/**
 * Config file requirements:
 * - execution mode = local, master, etc.
 * - app name to be deployed to
 */
public class SparkExporter extends Connector {
    // export from HDFS to Spark SQL
    private String executionMode;
    private String appName;

    public SparkExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.executionMode = (String) config.get("executionMode");
        this.appName = (String) config.get("appName");
    }

    public void execute() {
        // connect to spark sql url, run query on it
        SparkContext conf = new SparkContext(this.executionMode, this.appName);
        // TODO: Fix the file path to hdfs

        // psuedocode for adding files in directory:
        // for each file in output directory:
        // addFile from that filepath to Spark context
        conf.addFile("hdfs://output/");
    }
}
