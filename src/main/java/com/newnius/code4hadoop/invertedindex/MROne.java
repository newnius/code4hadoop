package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/17/17.
 *
 * bin/hadoop jar /mnt/jars/hadoop-tutorial-1.0-SNAPSHOT.jar com.newnius.code4hadoop.invertedindex.MROne InvertedIndex inverted/input inverted/output1
 */
public class MROne {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <mr name> <input path> <output path>");
            System.exit(-1);
        }

        String name = args[0];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MROne.class);

        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
