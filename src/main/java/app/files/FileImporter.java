package app.files;

import org.json.simple.JSONObject;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

public class FileImporter extends Connector {
    private String inputFilepath;
    private String HDFSaddress;
    private int HDFSport;

    public FileImporter(JSONObject config) {
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
            HDFSUtils.copyFromLocal(inputFilepath, Constants.WORKING_DIR, this.HDFSaddress, this.HDFSport);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }
}
