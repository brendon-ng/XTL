package app.hive;
import org.json.simple.JSONObject;
import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

// for Hive
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveExporter extends Connector
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String address;
    private String port;
    private String tableName;
    private String tableColumns;
    private String dockerContainerID;

    public HiveExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
        this.address = (String) config.get("address");
        this.port = (String) config.get("port");
        this.tableName = (String) config.get("tableName");
        this.tableColumns = (String) config.get("tableColumns");
        this.dockerContainerID = (String) config.get("dockerContainerID");
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
            // copy CSV from HDFS to local "data/hive/hive_output.csv"
            String localCSVdest = "data/hive/hive_output.csv";
            try {
                HDFSUtils.copyToLocal(Constants.OUTGOING_DIR, localCSVdest, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // establish connection to Hive
            String connectionUrl = String.format("jdbc:hive2://%s:%s", this.address, this.port);
            Connection con = DriverManager.getConnection(connectionUrl, "", "");
            Statement statement = con.createStatement();

            // drop table if exists
            String dropTableIfExistsQuery = String.format("DROP TABLE IF EXISTS %s", this.tableName);
            statement.execute(dropTableIfExistsQuery);

            // create table
            String createTableQuery = String.format("CREATE TABLE %s %s row format delimited fields terminated by ','", this.tableName, this.tableColumns);
            statement.execute(createTableQuery);

            // execute a shell command to copy CSV into the docker container
            ProcessBuilder processBuilder = new ProcessBuilder();

            String dockerCSVPath = "/opt/hive/hive_input.csv";
            String dockerPath = this.dockerContainerID + ":" + dockerCSVPath;
            processBuilder.command("docker", "cp", localCSVdest, dockerPath);

            try {

                Process process = processBuilder.start();
                process.waitFor();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            // load CSV data into table
            String loadCSVDataQuery = String.format(
                "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s",
                dockerCSVPath,
                this.tableName);
            statement.execute(loadCSVDataQuery);
            
            // close connections
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
