package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class ReducerFive extends Reducer<Text, IntWritable, Text, Text> {
    private int sum = 0;
    private String lastWord = null;
    private String lastDoc = null;
    private int docCount = 0;
    private String lastAuthor = null;

    private HashMap<String, Integer> totalDocs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalDocs = new HashMap<>();
        totalDocs.put("古龙", 70);
        totalDocs.put("卧龙生", 54);
        totalDocs.put("李凉", 41);
        totalDocs.put("梁羽生", 38);
        totalDocs.put("金庸", 15);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] pair = key.toString().split(",");
        String word = pair[1];

        if(lastWord!=null && !word.equals(lastWord)){
            double IDF = totalDocs.get(pair[0]) / (docCount + 1);
            IDF = Math.log(IDF) / Math.log(2);

            context.write(new Text(pair[0]+","+lastWord), new Text(String.valueOf(sum) + "-" + IDF));
            sum = 0;
            lastDoc = null;
            docCount = 0;
        }

        if(lastDoc == null || !pair[2].equals(lastDoc)){
            docCount ++;
        }

        for(IntWritable iw: values) {
            sum += iw.get();
        }

        lastWord = word;
        lastDoc = pair[2];
        lastAuthor = pair[0];
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(lastWord!=null){
            double IDF = totalDocs.get(lastAuthor) / (docCount + 1);
            IDF = Math.log(IDF);
            context.write(new Text(lastAuthor+","+lastWord), new Text(String.valueOf(sum) + "-" + IDF));
        }
    }
}
