package app;
import org.json.simple.JSONObject;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveImporter extends Connector
{
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    private String query;

    public HiveImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    protected void parseJSON(JSONObject config) {
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
            System.out.println("uh oh");
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("testing");
    }


}
