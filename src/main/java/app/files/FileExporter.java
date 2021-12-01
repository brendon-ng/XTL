package app.files;

import org.json.simple.JSONObject;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

public class FileExporter extends Connector {
    private String outputFilepath;

    public FileExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.outputFilepath = (String) config.get("outputFilepath");
    }

    @Override
    public void execute() {
        try {
            HDFSUtils.copyFromLocal(Constants.OUTGOING_DIR, this.outputFilepath, Constants.HDFS_WORKING_ADDR,
                    Constants.HDFS_WORKING_PORT);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }
}
