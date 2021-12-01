package app.files;

import org.json.simple.JSONObject;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

public class FileImporter extends Connector {
    private String inputFilepath;

    public FileImporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.inputFilepath = (String) config.get("inputFilepath");
    }

    @Override
    public void execute() {
        try {
            HDFSUtils.copyFromLocal(inputFilepath, Constants.WORKING_DIR, Constants.HDFS_WORKING_ADDR,
                    Constants.HDFS_WORKING_PORT);
        } catch (Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }
}
