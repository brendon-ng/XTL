
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.spark.sql.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

    static public void extractData(String platform, JSONObject config) throws IOException {
        switch (platform) {
            case "SPARKSQL":
                extractSparkSql((String) config.get("executionMode"), (String) config.get("inputFilepath"),
                        (String) config.get("query"));
                break;
            // add other cases here
            default:
                break;
        }
    }

    public static void main(String[] args) throws IOException {
        // command line arguments - args[0] should be the file path to config file
        if (args.length != 1) {
            IOException e = new IOException("Incorrect argument length");
            throw e;
        }

        // https://howtodoinjava.com/java/library/json-simple-read-write-json-examples/
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(args[0])) {
            // Read JSON file
            Object obj = jsonParser.parse(reader);

            JSONObject XTLConfig = (JSONObject) obj;
            // platform should be common across all sources
            String inputPlatform = (String) XTLConfig.get("platform");
            // System.out.println(XTLConfig);

            // run function based on platform to extract data
            extractData(inputPlatform, XTLConfig);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
