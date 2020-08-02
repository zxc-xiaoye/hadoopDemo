package com.zxc.mr.reducerjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean table = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");

        if(name.startsWith("order")) {

            table.setId(fields[0]);
            table.setPid(fields[1]);
            table.setAmount(Integer.parseInt(fields[2]));
            table.setFlag("order");
            table.setPname("");

            k.set(fields[1]);
        } else {
            table.setId("");
            table.setPid(fields[0]);
            table.setAmount(0);
            table.setFlag("pd");
            table.setPname(fields[1]);

            k.set(fields[0]);
        }
        context.write(k, table);
    }
}
