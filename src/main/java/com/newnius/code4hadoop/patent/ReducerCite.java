package com.newnius.code4hadoop.patent;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/19/17.
 *
 */
public class ReducerCite extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder("");
        for(Text val: values){
            sb.append(val.toString()).append(",");
        }
        String cites = sb.toString();
        if(cites.length() > 0){
            cites = cites.substring(0, cites.length() -1); //remove last ','
        }
        context.write(key, new Text(cites));
    }
}
