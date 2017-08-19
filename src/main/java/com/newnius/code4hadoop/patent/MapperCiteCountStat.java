package com.newnius.code4hadoop.patent;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/19/17.
 */
public class MapperCiteCountStat extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        context.write(new Text(line[1]), one);
    }
}
