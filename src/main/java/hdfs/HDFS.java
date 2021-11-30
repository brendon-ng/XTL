package hdfs;

import utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HDFS {
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
        createDir("/example");
    }

    // Send working directory data to final location
    public void load(String filepath) throws Exception {
        if(filepath.isEmpty()) {
            throw new Exception("HDFS: No load filepath specified");
        }
        copyDirectory(Constants.WORKING_DIR, filepath);
    }
}