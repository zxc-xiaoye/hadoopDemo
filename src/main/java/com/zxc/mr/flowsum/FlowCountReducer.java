package com.zxc.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUpFlow = 0L;
        long totalDownFlow = 0L;

        for (FlowBean flowBean : values) {
            totalUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFlow();
        }
        v.set(totalUpFlow, totalDownFlow);

        context.write(key, v);
    }
}
