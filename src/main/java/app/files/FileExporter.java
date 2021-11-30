package app.files;

import org.json.simple.JSONObject;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

public class FileExporter extends Connector {
    private String outputFilepath;
    private String HDFSaddress;
    private int HDFSport;

    public FileExporter(JSONObject config) {
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
            HDFSUtils.copyFromLocal(Constants.WORKING_DIR, this.outputFilepath, this.HDFSaddress, this.HDFSport);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }
}
