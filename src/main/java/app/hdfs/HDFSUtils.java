package app.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSUtils {
    public static FileSystem getFileSystem(String address, int port) throws Exception {
        Configuration configuration = getConfiguration(address, port);
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    public static Configuration getConfiguration(String address, int port) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", String.format("hdfs://%s:%d", address, port));
        return configuration;
    }

    public static void copyDirectory(String srcDir, String dstDir, String address, int port) throws Exception {
        FileSystem fileSystem = HDFSUtils.getFileSystem(address, port);
        Configuration configuration = HDFSUtils.getConfiguration(address, port);
        FileUtil.copy(fileSystem, new Path(srcDir), fileSystem, new Path(dstDir), false, configuration);
    }

    public static void rename(String src, String dst, String address, int port) throws Exception {
        FileSystem fileSystem = HDFSUtils.getFileSystem(address, port);
        fileSystem.rename(new Path(src), new Path(dst));
    }

    // create directory with name "dir"
    public static void createDir(String dir, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path path = new Path(dir);
        fileSystem.mkdirs(path);
    }

    // delete directory with name "dir"
    public static void deleteDir(String dir, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path path = new Path(dir);
        fileSystem.delete(path, true);
    }

    public static void createFile(String filePath, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path path = new Path(filePath);
        fileSystem.create(path);
    }

    public static void copyFromLocal(String src, String dst, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path source = new Path(src);
        Path destination = new Path(dst);
        fileSystem.copyFromLocalFile(source, destination);
    }

    public static void copyToLocal(String src, String dst, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path source = new Path(src);
        Path destination = new Path(dst);
        fileSystem.copyToLocalFile(source, destination);
    }

    public static boolean checkExists(String path, String address, int port) throws Exception {
        FileSystem fileSystem = getFileSystem(address, port);
        Path target = new Path(path);
        return fileSystem.exists(target);
    }
}
