package com.bigdata.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Flow, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {

        int up = 0;
        int down = 0;
        int total = 0;
        for (Flow value: values) {
            up += value.getUp();
            down += value.getDown();
            total += value.getTotal();
        }

        context.write(key, new Text(String.format("%d\t%d\t%d", up, down, total)));
    }
}
