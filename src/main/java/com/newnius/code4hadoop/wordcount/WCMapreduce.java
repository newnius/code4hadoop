package com.newnius.code4hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/11/17.
 *
 */
public class WCMapreduce {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(WCMapreduce.class);
        job.setJobName("WordCount");

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
