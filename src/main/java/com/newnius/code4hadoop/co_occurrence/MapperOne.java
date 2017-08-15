package com.newnius.code4hadoop.co_occurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by newnius on 8/15/17.
 *
 */
public class MapperOne extends Mapper<Object, Text, Pair, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable one = new IntWritable(1);
        int windowSize = context.getConfiguration().getInt("windowSize", 0);

        Queue<String> queue = new LinkedList<>();

        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(value.toString());
        while (matcher.find()) {
            queue.add(matcher.group());
            if(queue.size() >= windowSize){
                String wordA = null;
                for(String wordB: queue){
                    if(wordA!=null) {
                        context.write(new Pair(wordA, wordB), one);
                    }
                    wordA =wordB;
                }
                queue.remove();
            }
        }

        while(queue.size()>0){
            String wordA = null;
            for(String wordB: queue){
                if(wordA!=null) {
                    context.write(new Pair(wordA, wordB), one);
                }
                wordA =wordB;
            }
            queue.remove();
        }
    }
}
