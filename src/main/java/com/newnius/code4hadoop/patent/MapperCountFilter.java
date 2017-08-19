package com.newnius.code4hadoop.patent;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/19/17.
 *
 */
public class MapperCountFilter extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private String field;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        field = context.getConfiguration().get("field");
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        String year = line[1];
        String country = line[4].replaceAll("\"", "");
        if(year.equals("\"GYEAR\"")){
            return;
        }
        if(field.equals("country")) {
            context.write(new Text(country), one);
        }else {
            context.write(new Text(year), one);
        }
    }
}
