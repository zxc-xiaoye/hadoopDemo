package com.zxc.mr.filter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream zxcfos;
    private FSDataOutputStream otherfos;
    public FilterRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            this.zxcfos = fs.create(new Path("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/output/zxc.log"));
            this.otherfos = fs.create(new Path("/Users/zhangxiaochi/IdeaProjects/hadoopDemo/src/main/java/output/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().contains("zxc")) {
            zxcfos.write(text.toString().getBytes());
        } else {
            otherfos.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(zxcfos);
        IOUtils.closeStream(otherfos);
    }
}
