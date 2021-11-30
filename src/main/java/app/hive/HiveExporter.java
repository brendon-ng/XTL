package app.hive;
import org.json.simple.JSONObject;
import app.Connector;

// for Hive
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveExporter extends Connector
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String address;
    private String port;
    private String inputCSVPath;
    private String tableName;
    private String tableColumns;

    public HiveExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.address = (String) config.get("address");
        this.port = (String) config.get("port");
        this.inputCSVPath = (String) config.get("inputCSVPath");
        this.tableName = (String) config.get("tableName");
        this.tableColumns = (String) config.get("tableColumns");
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
            String connectionUrl = String.format("jdbc:hive2://%s:%s", this.address, this.port);
            Connection con = DriverManager.getConnection(connectionUrl, "", "");
            Statement statement = con.createStatement();

            // drop table if exists
            String dropTableIfExistsQuery = String.format("DROP TABLE IF EXISTS %s", this.tableName);
            statement.execute(dropTableIfExistsQuery);

            // create table
            String createTableQuery = String.format("CREATE TABLE %s %s", this.tableName, this.tableColumns);
            statement.execute(createTableQuery);

            // load CSV data into table
            // TODO: figure this out because the path is in the container so we need to get the csv in the container
            /*
            String loadCSVDataQuery = String.format(
                "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s",
                this.inputCSVPath,
                this.tableName);
            statement.execute(loadCSVDataQuery);
            */

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
