package app;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import app.SparkSQL.*;
import app.hdfs.*;

public class App {

    private static JSONObject config;
    private static Connector importer;
    private static Connector exporter;

    private static void run(String[] args) throws IOException {
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

            config = (JSONObject) obj;

            JSONObject importerConfig = (JSONObject) config.get("importer");
            JSONObject transformationConfig = (JSONObject) config.get("transformation");
            JSONObject exporterConfig = (JSONObject) config.get("exporter");

            switch ((String) importerConfig.get("platform")) {
                case "SPARKSQL":
                    importer = new SparkSQLImporter(importerConfig);
                    break;
                case "HDFS":
                    importer = new HDFSImporter(importerConfig);
                    break;
                default:
                    System.out.println("Invalid Importer Platform");
                    break;
            }

            switch ((String) exporterConfig.get("platform")) {
                default:
                    System.out.println("Invalid Exporter Platform");
                    break;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        importer.execute();
        // transformation.execute();
        // exporter.execute();
    }

    public static void main(String[] args) throws IOException {
        run(args);
    }
}
