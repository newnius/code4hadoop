package com.newnius.code4hadoop.knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/21/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.knn.MROne knn/input knn/output knn/train 7
 */
public class MROne {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 4) {
            System.err.println("Usage: <input path> <output path> <train_file_path> <k>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("k", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(MROne.class);

        job.addCacheFile(new Path(args[2]).toUri());

        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
