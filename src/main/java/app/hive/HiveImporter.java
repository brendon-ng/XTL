package app.hive;

import org.json.simple.JSONObject;

// for Hive
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

// for writing to a CSV
import java.io.File;
//import com.opencsv.CSVWriter;
import java.io.FileWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import app.Connector;

public class HiveImporter extends Connector {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String port;
    private String query;
    private String address;

    public HiveImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.port = (String) config.get("port");
        this.query = (String) config.get("query");
        this.address = (String) config.get("address");
    }

    public void execute() {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            // establish connection to Hive
            String connectionUrl = String.format("jdbc:hive2://%s:%s", this.address, this.port);
            Connection con = DriverManager.getConnection(connectionUrl, "", "");
            Statement statement = con.createStatement();

            // execute query
            ResultSet result = statement.executeQuery(this.query);

            /*
             * while (result.next()) {
             * System.out.println("Col1=" + result.getString(1));
             * System.out.println("Col2=" + result.getString(2));
             * }
             */
            
            // trying https://idineshkrishnan.com/convert-resultset-to-csv-in-java/

            // write the result to a csv
            try {
                String csvOutputFileName = "data/hive/hive_output.csv";
                File csvOutputFileObj = new File(csvOutputFileName);

                if (!csvOutputFileObj.getParentFile().exists()) {
                    csvOutputFileObj.getParentFile().mkdirs();
                }

                csvOutputFileObj.delete();

                csvOutputFileObj.createNewFile();

                // https://idineshkrishnan.com/convert-resultset-to-csv-in-java/

                // creating the csv format
                CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");

                // creating the file object
                File file = new File(csvOutputFileName);

                // creating file writer object
                FileWriter fw = new FileWriter(file);

                // creating the csv printer object
                CSVPrinter printer = new CSVPrinter(fw, format);

                // printing the result in 'CSV' file
                printer.printRecords(result);

                // closing all resources
                fw.close();
                printer.close();

                /*
                CSVWriter csvWriter = new CSVWriter(new FileWriter(csvOutputFileName));
                csvWriter.writeAll(result, true);
                csvWriter.flush();
                csvWriter.close();
                */
            } catch (Exception e) {
                e.printStackTrace();
            }

            result.close();
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
