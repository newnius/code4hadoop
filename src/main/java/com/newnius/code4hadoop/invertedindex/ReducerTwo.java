package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.util.InternCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class ReducerTwo extends Reducer<Text, IntWritable, Text, Text> {
    private int sum = 0;
    private String lastWord = null;
    private HashMap<String, Integer> cnt = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] pair = key.toString().split(",");
        String word = pair[0];

        if(lastWord!=null && !word.equals(lastWord)){
            StringBuilder sb = new StringBuilder();
            sb.append((float)sum / cnt.size()).append(",");
            for(Map.Entry<String, Integer> entry : cnt.entrySet()){
                sb.append(entry.getKey().split(",")[1].replace(".txt.segmented", "").replace(".TXT.segmented", "")).append(":").append(entry.getValue()).append(";");
            }
            context.write(new Text(lastWord), new Text(sb.toString().substring(0, sb.length()-1)));
            sum = 0;
            cnt.clear();
        }

        for(IntWritable iw: values) {
            Integer t = cnt.get(key.toString());
            if (t == null) {
                t = 0;
            }
            cnt.put(key.toString(), t + iw.get());
            sum += iw.get();
        }

        lastWord = word;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(lastWord!=null){
            StringBuilder sb = new StringBuilder();
            sb.append((float)sum / cnt.size()).append(",");
            for(Map.Entry<String, Integer> entry : cnt.entrySet()){
                sb.append(entry.getKey().split(",")[1].replace(".txt.segmented", "").replace(".TXT.segmented", "")).append(":").append(entry.getValue()).append(";");
            }
            context.write(new Text(lastWord), new Text(sb.toString().substring(0, sb.length()-1)));
        }
    }
}
