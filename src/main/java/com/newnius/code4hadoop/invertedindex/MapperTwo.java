package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class MapperTwo extends Mapper<Object, Text, Text, IntWritable> {
//    private Set<String> skipWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        URI[] uris = context.getCacheFiles();
//        for(URI uri: uris){
//            String line;
//            BufferedReader br = new BufferedReader(new FileReader(new Path(uri).getName()));
//            while((line = br.readLine()) != null){
//                StringTokenizer itr = new StringTokenizer(line);
//                while(itr.hasMoreTokens()){
//                    skipWords.add(itr.nextToken());
//                }
//            }
//        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words= value.toString().split(" ");
        String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
        //if(!skipWords.contains(fields[3])) {
        for (String word: words)
            context.write(new Text(word + "," + filename), new IntWritable(1));
        //}
    }
}
