package app.hdfs;

import org.json.simple.JSONObject;

import app.Connector;
import app.utils.Constants;

public class HDFSExporter extends Connector{
    private String outputFilepath;
    private String HDFSaddress;
    private int HDFSport;

    public HDFSExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.outputFilepath = (String) config.get("outputFilepath");
        this.HDFSaddress = (String) config.get("address");
        this.HDFSport = Integer.parseInt((String) config.get("port"));
    }

    @Override
    public void execute() {
        try {
            load(outputFilepath);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }

    // Send working directory data to final location
    public void load(String filepath) throws Exception {
        if (filepath.isEmpty()) {
            throw new Exception("HDFS: No load filepath specified");
        }
        HDFSUtils.copyDirectory(Constants.WORKING_DIR, filepath, this.HDFSaddress, this.HDFSport);
    }
}
