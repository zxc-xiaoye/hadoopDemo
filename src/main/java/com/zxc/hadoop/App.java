package com.zxc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Hello world!
 *
 */
public class App {
    static final String HDFS_PATH = "hdfs://holy:9000";


    public static void main( String[] args ) throws Exception {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");
//        fs.mkdirs(new Path("/sanguo/666"));
//        fs.close();
//        testCopyFromLocalFile();
//        testCopyToLocal();
        delete();
        System.out.println( "Hello World!" );
    }

    public static void testCopyFromLocalFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");

        fs.copyFromLocalFile(new Path("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/caocao.txt"), new Path("/sanguo/caocao.txt"));
        fs.close();
    }

    private static void testCopyToLocal()throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");
        fs.copyToLocalFile(new Path("/sanguo/caocao.txt"), new Path("./666.txt"));
        fs.close();


    }
    private static void delete()throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");
        fs.delete(new Path("/sanguo/666"), true);
        fs.close();
    }
}
