import java.io.IOException;
import org.apache.spark.sql.*;
import org.json.simple.JSONObject;

public class SparkSQLImporter extends Connector{
    private String executionMode;
    private String inputFilePath;
    private String query;

    public SparkSQLImporter(JSONObject config){
        super(config);
    }

    protected void parseJSON(JSONObject config){
        String executionMode = (String) config.get("exectionMode");
        String inputFilePath = (String) config.get("inputFilePath");
        String query = (String) config.get("query");
    }

    public void execute(){
        // connect to spark sql url, run query on it
        SparkSession spark = SparkSession.builder().appName("XTL Spark SQL app").master(executionMode).getOrCreate();
        Dataset<Row> df = spark.read().option("header", "true").csv(inputFilePath);
        df.createOrReplaceTempView("XTLInput");

        // run initial query on file
        Dataset<Row> sqlQuery = spark.sql(query);

        // save results in CSV directory - use *.csv to get file with results
        sqlQuery.write().csv("data/sparksqlExtracted.csv");
    }
}
