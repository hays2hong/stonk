package edu.hhu.stonk.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


public class HDFSClient implements Closeable {
    private FileSystem fs;

    /***
     * @param user 传入需要连接的用户
     * @throws Exception
     */
    public HDFSClient(String HdfsUri, String user) throws Exception {
        Configuration configuration = new Configuration();
        //HDFS 的地址需要修改成自己的地址
        configuration.set("fs.default.name", HdfsUri);
        // 设定文件系统的URI, 配置, 以及用户
        fs = FileSystem.get(new URI(HdfsUri), configuration, user);
    }

    /***
     * 默认初始化root用户
     * @throws Exception
     */
    public HDFSClient(String HdfsUri) throws Exception {
        Configuration configuration = new Configuration();
        //HDFS 的地址需要修改成自己的地址
        configuration.set("fs.default.name", HdfsUri);
        //设定文件系统的URI, 配置, 以及用户
        fs = FileSystem.get(new URI(HdfsUri), configuration, "root");
    }

    /**
     * 删除HDFS 中指定的目录
     *
     * @param path 需要删除的目录
     * @param is   是否进行递归删除文件
     * @throws IOException
     */
    public boolean delete(String path, boolean is) throws IOException {
        boolean res = true;
        if (fs.exists(new Path(path))) {
            res = fs.delete(new Path(path), is);
        }
        return res;
    }


    /**
     * 列出该路径下的所有文件
     * @param path
     * @param recursive 是否递归
     * @return
     * @throws IOException
     */
    public List<String> list(String path, boolean recursive) throws IOException {
        List<String> sons = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(path), recursive);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            sons.add(file.getPath().toString());

        }
        return sons;
    }

    public List<String> list(String path) throws IOException {
        return list(path, false);
    }

    /***
     * 文件从本地上传到
     * HDFS
     * @param src
     * @param dst
     * @return
     */
    public boolean uploadFromLocal(String src, String dst) {
        return uploadFromLocal(src, dst, false);
    }

    /***
     * 文件从本地上传到
     * HDFS
     * @param src
     * @param dst
     * @return
     */
    public boolean uploadFromLocal(String src, String dst, boolean delSrc) {
        try {
            fs.copyFromLocalFile(delSrc, true, new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 文件下载
     *
     * @param src
     * @param dst
     * @return
     */
    public boolean getFromHDFS(String src, String dst) {
        Path dstPath = new Path(dst);
        try {
            fs.copyToLocalFile(false, new Path(src), dstPath);
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    public DataInputStream getFileInputStream(String src) throws IOException {
        return fs.open(new Path(src));
    }

    public boolean mkdir(String path) {
        try {
            return fs.mkdirs(new Path(path));
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
    }

    public boolean exists(String path) throws IOException {
        return fs.exists(new Path(path));
    }

    public DatanodeInfo[] getNodeInfo() throws IOException {
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        return hdfs.getDataNodeStats();
    }

    @Override
    public void close() throws IOException {
        fs.close();
    }
}
