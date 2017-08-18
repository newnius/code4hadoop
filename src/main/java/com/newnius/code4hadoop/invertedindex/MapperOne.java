package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/16/17.
 *
 */
public class MapperOne extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        context.write(new Text(fields[3]), new Text(fields[2]));
    }
}
