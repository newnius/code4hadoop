package com.newnius.code4hadoop.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class MROne {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <input path> <output path> <cluster_file_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("cluster_file", args[2]);

        Job job = Job.getInstance(conf, "K-Means");
        job.setJarByClass(MROne.class);

        job.setMapperClass(MapperOne.class);
        job.setCombinerClass(ReducerOne.class);
        job.setReducerClass(ReducerOne.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Cluster.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Cluster.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
