package app.hdfs;

import org.json.simple.JSONObject;
import org.apache.hadoop.fs.*;

import app.Connector;
import app.utils.Constants;

public class HDFSExporter extends Connector {
    private String outputFilepath;
    private String HDFSaddress;
    private int HDFSport;
    private String transformId;

    public HDFSExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.outputFilepath = (String) config.get("outputFilepath");
        this.HDFSaddress = (String) config.get("address");
        this.HDFSport = Integer.parseInt((String) config.get("port"));
        this.transformId = (String) config.get("transformId");
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

        String path = String.format("hdfs://%s:%d%s", Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT,
                Constants.OUTGOING_DIR + "_" + this.transformId);
        try {
            HDFSUtils.createDir(filepath, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
            FileSystem fs = HDFSUtils.getFileSystem(Constants.HDFS_WORKING_ADDR,
                    Constants.HDFS_WORKING_PORT);

            FileStatus listFiles[] = fs.listStatus(new Path(path));
            Path filepaths[] = FileUtil.stat2Paths(listFiles);
            for (int i = 0; i < filepaths.length; i++) {
                HDFSUtils.copyDirectory(filepaths[i].toString(), filepath, Constants.HDFS_WORKING_ADDR,
                        Constants.HDFS_WORKING_PORT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
