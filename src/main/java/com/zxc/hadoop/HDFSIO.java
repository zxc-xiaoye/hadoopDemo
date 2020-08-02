package com.zxc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HDFSIO {


    static final String HDFS_PATH = "hdfs://holy:9000";


    public static void main(String[] args) throws Exception {
//        putFileToHDFS();
        getFileFromHDFS();
    }
    private static void putFileToHDFS() throws Exception{
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");

        conf.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");

        FileInputStream fis = new FileInputStream(new File("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/caocao.txt"));

        FSDataOutputStream fos = fs.create(new Path("/sanguo/caocaocaocao.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    private static void getFileFromHDFS()throws Exception {
        Configuration conf = new Configuration();

        conf.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");
        FSDataInputStream fis = fs.open(new Path("/sanguo/caocaocaocao.txt"));
        FileOutputStream fos = new FileOutputStream(new File("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/caocaodownload.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
