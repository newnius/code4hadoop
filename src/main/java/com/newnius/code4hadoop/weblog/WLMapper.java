package com.newnius.code4hadoop.weblog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by newnius on 8/11/17.
 *
 */
public class WLMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String field = context.getConfiguration().get("field");

        String  record = value.toString();
        String[] items = record.split(" ");
        String v = null;
        switch(field){
            case "ip":
                v = items[0];
                break;
            case "time":
                v = items[3] + items[4];
                break;
            case "method":
                v = items[5].replace("\"", "");
                break;
            case "url":
                v = items[6];
                break;
            case "version":
                v = items[7].replace("\"", "");
                break;
            case "response":
                v = items[8];
                break;
            case "size":
                v = items[9];
                break;
            case "referer":
                v = items[10].replaceAll("\"", "");
                break;
            case "ua":
                v = record.substring(record.substring(0, record.length()-1).lastIndexOf("\""));
                v = v.substring(0, v.length() - 1);
                break;
        }
        context.write(new Text(v), new IntWritable(1));
    }
}
