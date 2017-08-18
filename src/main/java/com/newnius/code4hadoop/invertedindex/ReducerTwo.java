package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class ReducerTwo extends Reducer<Text, IntWritable, Text, Text> {
    private String lastWord = null;
    private List<String> items = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: values){
            sum += val.get();
        }
        String[] pair = key.toString().split(",");
        String item = pair[1] + "," + sum;
        if(lastWord!=null && !lastWord.equals(pair[0])){
            //int count = 0;
            StringBuilder sb = new StringBuilder();
            for(String i: items){
                sb.append(i);
                sb.append(";");
                //count += Long.parseLong(i.split(";")[0]);
            }
            context.write(new Text(pair[0]), new Text(sb.toString()));
            items = new ArrayList<>();
        }
        items.add(item);
        lastWord = pair[0];
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(String i: items){
            sb.append(i);
            sb.append(";");
            //count += Long.parseLong(i.split(";")[0]);
        }
        context.write(new Text(lastWord), new Text(sb.toString()));
    }
}
