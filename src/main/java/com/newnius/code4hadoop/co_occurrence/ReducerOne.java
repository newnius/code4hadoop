package com.newnius.code4hadoop.co_occurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 */
public class ReducerOne extends Reducer<Pair, IntWritable, Pair, IntWritable>{
    @Override
    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val: values){
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
