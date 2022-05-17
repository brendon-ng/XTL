package app.transform;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.json.simple.JSONObject;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.lang.ProcessBuilder;

import app.Connector;
import app.utils.Constants;
import app.hdfs.HDFSUtils;

public class Transformation extends Connector {
    private String mapper;
    private String reducer;
    private String filenameUpdate;

    public Transformation(JSONObject config) {
        super(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.mapper = (String) config.get("mapper");
        this.reducer = (String) config.get("reducer");
        this.filenameUpdate = config.containsKey("filename") ? (String) config.get("filename") : "";
    }

    private Path[] filterPaths(Path[] pArray) {
        Path[] filtered = new Path[pArray.length - 1];
        for (int i = 0; i < pArray.length; i++) {
            String pName = pArray[i].getName();
            if (!pName.matches("part.*")) {
                filtered = ArrayUtils.remove(pArray, i);
            }
        }
        return filtered;
    }

    @Override
    public void execute() {
        try {
            Process p = new ProcessBuilder("mapred", "streaming",
                    "-input", Constants.WORKING_DIR,
                    "-output", Constants.OUTGOING_DIR,
                    "-mapper", mapper,
                    "-reducer", reducer).start();
            p.waitFor();

            if (!filenameUpdate.equals("")) {
                String updatedFilepath = String.format("%s/%s",
                        Constants.OUTGOING_DIR, this.filenameUpdate);

                FileSystem fs = HDFSUtils.getFileSystem(Constants.HDFS_WORKING_ADDR,
                        Constants.HDFS_WORKING_PORT);
                Path updatedPath = new Path(updatedFilepath);
                Path workingDir = new Path(Constants.OUTGOING_DIR);
                Path[] processedFiles = FileUtil.stat2Paths(fs.listStatus(workingDir));
                Path[] updatedProcessedFiles = filterPaths(processedFiles);

                fs.create(updatedPath).close();
                fs.concat(updatedPath, updatedProcessedFiles);
            }
        } catch (IOException e) {
            System.out.println(e);
        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
