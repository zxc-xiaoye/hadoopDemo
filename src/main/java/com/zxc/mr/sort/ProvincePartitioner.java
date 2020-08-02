package com.zxc.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int partitions) {
        String numPre = text.toString().substring(0, 3);

        if("134".equals(numPre)) {
            return 0;
        } else if ("136".equals(numPre)) {
            return 1;
        } else if ("138".equals(numPre)) {
            return 2;
        }
        return 3;
    }
}
