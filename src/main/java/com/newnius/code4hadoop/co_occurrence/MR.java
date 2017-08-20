package com.newnius.code4hadoop.co_occurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.co_occurrence.MRCluster co-occurrence coOccurrence/input coOccurrence/output 10
 *
 */
public class MR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 4) {
            System.err.println("Usage: <mr name> <input path> <output path> <windowSize>");
            System.exit(-1);
        }

        String name = args[0];

        Configuration conf = new Configuration();
        conf.setInt("windowSize", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MR.class);

        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
