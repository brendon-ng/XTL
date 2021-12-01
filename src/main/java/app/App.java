package app;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import app.SparkSQL.*;
import app.files.*;
import app.hdfs.*;
import app.hive.HiveImporter;
import app.transform.*;
import app.utils.Constants;

public class App {

    private static JSONObject config;
    private static Connector importer;
    private static Connector transformation;
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
                case "HIVE":
                    BasicConfigurator.configure();
                    importer = new HiveImporter(importerConfig);
                    break;
                case "HDFS":
                    importer = new HDFSImporter(importerConfig);
                    break;
                case "FILE":
                    importer = new FileImporter(importerConfig);
                    break;
                default:
                    System.out.println("Invalid Importer Platform");
                    break;
            }

            if (transformationConfig != null) {
                transformation = new Transformation(transformationConfig);
            }

            switch ((String) exporterConfig.get("platform")) {
                case "HDFS":
                    exporter = new HDFSExporter(exporterConfig);
                    break;
                case "FILE":
                    exporter = new FileExporter(exporterConfig);
                    break;
                case "SPARK":
                    exporter = new SparkExporter(exporterConfig);
                    break;
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

        // Set HDFS environment
        try {
            HDFSUtils.deleteDir(Constants.WORKING_DIR, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
            HDFSUtils.createDir(Constants.WORKING_DIR, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
            HDFSUtils.deleteDir(Constants.OUTGOING_DIR, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("EXECUTING IMPORTER");
        importer.execute();

        if (transformation != null) {
            System.out.println("EXECUTING TRANSFORMATION");
            transformation.execute();
        } else {
            System.out.println("NO TRANSFORMATION CONFIG FOUND: No transform will be performed");
            try {
                HDFSUtils.rename(Constants.WORKING_DIR, Constants.OUTGOING_DIR, Constants.HDFS_WORKING_ADDR,
                        Constants.HDFS_WORKING_PORT);
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                System.exit(1);
            }
        }

        System.out.println("EXECUTING EXPORTER");
        exporter.execute();
    }

    public static void main(String[] args) throws IOException {
        run(args);
    }
}
