package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class ReducerOne extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String cities = "";
        for(Text value: values){
            cities += value + ",";
        }
        cities = cities.substring(0, cities.length()-1);
        context.write(key, new Text(cities));
    }
}
