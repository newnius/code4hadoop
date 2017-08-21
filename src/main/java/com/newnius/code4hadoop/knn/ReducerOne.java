package com.newnius.code4hadoop.knn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class ReducerOne extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("k", 7);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer, Integer> count = new HashMap<>(k);

        for(Text val: values){
            String[] types = val.toString().split(",");
            for(String s: types) {
                int type = Integer.parseInt(s);
                if (!count.containsKey(type)){
                    count.put(type, 0);
                }
                int cnt = count.get(type);
                count.put(type, ++cnt);
            }
            int max = 0;
            int type = 0;
            for(Map.Entry<Integer, Integer> pair : count.entrySet()){
                if(pair.getValue() > max){
                    type = pair.getKey();
                    max = pair.getValue();
                }
            }
            context.write(key, new IntWritable(type));
        }
    }
}
