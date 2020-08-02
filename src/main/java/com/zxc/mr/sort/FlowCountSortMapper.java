package com.zxc.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        Text v = new Text();
        FlowBean k = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");

        v.set(fields[1]);

        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(upFlow, downFlow);

        context.write(k, v);
    }
}
