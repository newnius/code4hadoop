package com.newnius.code4hadoop.patent;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/19/17.
 *
 */
public class MapperCite extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        if(line[0].equals("\"CITING\"")){//skip header
            return;
        }
        context.write(new Text(line[1]), new Text(line[0]));
    }
}
