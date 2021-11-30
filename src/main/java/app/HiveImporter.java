package app;
import org.json.simple.JSONObject;

// for Hive
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

// for writing to a CSV
import java.io.File;
import com.opencsv.CSVWriter;
import java.io.FileWriter;

public class HiveImporter extends Connector
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String port;
    private String query;

    public HiveImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.port = (String) config.get("port");
        this.query = (String) config.get("query");
    }

    public void execute()
    {
        try
        {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    
        try
        {
            // establish connection to Hive
            String connectionUrl = String.format("jdbc:hive2://localhost:%s", this.port);
            Connection con = DriverManager.getConnection(connectionUrl, "", "");
            Statement statement = con.createStatement();

            // execute query
            ResultSet result = statement.executeQuery(this.query);

            /*
            while (result.next()) {
                System.out.println("Col1=" + result.getString(1));
                System.out.println("Col2=" + result.getString(2));
            }
            */

            // write the result to a csv
            try
            {
                String csvOutputFileName = "data/hive/hive_output.csv";
                File csvOutputFileObj = new File(csvOutputFileName);

                if (!csvOutputFileObj.getParentFile().exists())
                {
                    csvOutputFileObj.getParentFile().mkdirs();
                }

                csvOutputFileObj.delete();

                csvOutputFileObj.createNewFile();

                CSVWriter csvWriter = new CSVWriter(new FileWriter(csvOutputFileName));
                csvWriter.writeAll(result, true);
                csvWriter.flush();
                csvWriter.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            
            result.close();
            statement.close();
            con.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            System.exit(1);
        }    
    }
}
