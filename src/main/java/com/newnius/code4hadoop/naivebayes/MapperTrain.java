package com.newnius.code4hadoop.naivebayes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/22/17.
 *
 */
public class MapperTrain extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        context.write(new Text("class_" + line[0]), one);
        for(int i=1; i<line.length; i++){
            String t = "class_" + line[0] + "#col_" + i + "#" + line[i];
            context.write(new Text(t), one);
        }
    }
}
