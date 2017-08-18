package com.newnius.code4hadoop.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/18/17.
 *
 */
public class ReducerCal extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double pr = 0.0;
        String links = "";
        Double p = context.getConfiguration().getDouble("p", 0.85);
        for(Text val: values){
            String[] tuple = val.toString().split("\t");
            if(tuple.length > 1){
                if(tuple.length == 3) {
                    links = tuple[2];
                }
                continue;
            }
            pr += Double.parseDouble(val.toString());
        }
        pr = (1-p) + p * pr;
        context.write(key, new Text(pr+"\t"+links));
    }
}
