package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
    private Set<String> skipWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        for(URI uri: uris){
            String line;
            BufferedReader br = new BufferedReader(new FileReader(uri.getPath()));
            while((line = br.readLine()) != null){
                StringTokenizer itr = new StringTokenizer(line);
                while(itr.hasMoreTokens()){
                    skipWords.add(itr.nextToken());
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields= value.toString().split("\t");
        if(!skipWords.contains(fields[3])) {
            context.write(new Text(fields[3]+","+fields[2]), new IntWritable(1));
        }
    }
}
