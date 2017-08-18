package com.newnius.code4hadoop.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/18/17.
 *
 */
public class MapperCal extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\t");
        Double pr = Double.parseDouble(tuple[1]);
        if(tuple.length == 3) {
            String[] links = tuple[2].split(",");
            for (String link : links) {
                if (link.length() == 0) {
                    break;
                }
                context.write(new Text(link), new Text(pr / links.length + ""));
            }
        }
        context.write(new Text(tuple[0]), value);
    }
}
