package com.newnius.code4hadoop.pagerank;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/18/17.
 *
 */
public class MapperSorter extends Mapper<Object, Text, FloatWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\t");
        String url = tuple[0];
        Float pr = Float.parseFloat(tuple[1]);
        context.write(new FloatWritable(pr), new Text(url));
    }
}
