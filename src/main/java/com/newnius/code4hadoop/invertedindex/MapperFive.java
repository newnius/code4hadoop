package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class MapperFive extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words= value.toString().split(" ");
        String filename = ((FileSplit)context.getInputSplit()).getPath().getName().replace(".txt.segmented", "").replace(".TXT.segmented", "");
        String author = "";

        Pattern p = Pattern.compile("([0-9].{2})");
        Matcher m = p.matcher(filename);
        if(m.find()) {
            author = filename.substring(0, filename.indexOf(m.group()));
        }
        for (String word: words)
            context.write(new Text(author + "," + word + "," + filename), new IntWritable(1));
    }
}
