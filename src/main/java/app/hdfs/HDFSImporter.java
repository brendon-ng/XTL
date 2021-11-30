package app.hdfs;

import org.json.simple.JSONObject;

import java.lang.Integer;

import app.Connector;
import app.utils.Constants;

public class HDFSImporter extends Connector {
    private String inputFilepath;
    private String HDFSaddress;
    private int HDFSport;

    public HDFSImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.inputFilepath = (String) config.get("inputFilepath");
        this.HDFSaddress = (String) config.get("address");
        this.HDFSport = Integer.parseInt((String) config.get("port"));
    }

    @Override
    public void execute() {
        try {
            extract(inputFilepath);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }

    // Extract data from HDFS directory filepath to working directory
    public void extract(String filepath) throws Exception {
        if (filepath.isEmpty()) {
            throw new Exception("HDFS: No extract filepath specified");
        }
        HDFSUtils.copyDirectory(filepath, Constants.WORKING_DIR, this.HDFSaddress, this.HDFSport);
    }
}