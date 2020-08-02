package com.zxc.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int partitions) {
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
