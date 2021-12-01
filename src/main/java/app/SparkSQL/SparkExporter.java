package app.SparkSQL;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.json.simple.JSONObject;
import org.apache.hadoop.fs.*;

import app.Connector;
import app.hdfs.HDFSUtils;
import app.utils.Constants;

/**
 * Config file requirements:
 * - execution mode = local, master, etc.
 * - app name to be deployed to
 */
public class SparkExporter extends Connector {
    // export from HDFS to Spark
    private String executionMode;
    private String appName;

    public SparkExporter(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.executionMode = (String) config.get("executionMode");
        this.appName = (String) config.get("appName");
    }

    @Override
    public void execute() {
        // connect to spark sql url, run query on it

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(executionMode);
        SparkContext context = new SparkContext(conf);

        String path = String.format("hdfs://%s:%d%s", Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT,
                Constants.OUTGOING_DIR);
        try {
            FileSystem fs = HDFSUtils.getFileSystem(Constants.HDFS_WORKING_ADDR,
                    Constants.HDFS_WORKING_PORT);

            FileStatus listFiles[] = fs.listStatus(new Path(path));
            Path filepaths[] = FileUtil.stat2Paths(listFiles);
            for (int i = 0; i < filepaths.length; i++) {
                context.addFile(filepaths[i].toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
