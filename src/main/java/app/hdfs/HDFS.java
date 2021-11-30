package app.hdfs;

import org.json.simple.JSONObject;

import app.Connector;
import app.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HDFS extends Connector {
    private String inputFilePath;

    public HDFS(JSONObject config) {
        super(config);
        parseJSON(config);
    }

    private FileSystem getFileSystem() throws Exception {
        Configuration configuration = getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    private Configuration getConfiguration() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:8020");
        return configuration;
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.inputFilePath = (String) config.get("inputFilepath");
    }

    @Override
    public void execute() {
        // TODO Auto-generated method stub
        try{
            extract(inputFilePath);
        } catch(Exception e) {
            System.out.println("Error extracting from HDFS");
            System.out.println(e.toString());
        }
    }

    protected void copyDirectory(String srcDir, String dstDir) throws Exception {
        FileSystem fileSystem = getFileSystem();
        Configuration configuration = getConfiguration();
        FileUtil.copy(fileSystem, new Path(srcDir), fileSystem, new Path(dstDir), false, configuration);
    }

    // create directory with name "dir"
    protected void createDir(String dir) throws Exception {
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(dir);
        fileSystem.mkdirs(path);
    }

    // Extract data from HDFS directory filepath to working directory
    public void extract(String filepath) throws Exception {
        if (filepath.isEmpty()) {
            throw new Exception("HDFS: No extract filepath specified");
        }
        copyDirectory(filepath, Constants.WORKING_DIR);
    }

    // Send working directory data to final location
    public void load(String filepath) throws Exception {
        if(filepath.isEmpty()) {
            throw new Exception("HDFS: No load filepath specified");
        }
        copyDirectory(Constants.WORKING_DIR, filepath);
    }
}