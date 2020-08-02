package com.zxc.mr.cache;

import com.zxc.mr.flowsum.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        args = new String[]{"/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/input4", "/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/output5"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/input3/product.txt"));


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
