package com.newnius.code4hadoop.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 *
 *
 * bin/hadoop jar /mnt/jars/hadoop-tutorial-1.0-SNAPSHOT.jar com.newnius.code4hadoop.matrix.MRMulti matrix matrix/input matrix/output 20 30 40
 *
 */
public class MRMulti {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 6) {
            System.err.println("Usage: <mr name> <input path> <output path> <rowM> <columnM> <columnN>");
            System.exit(-1);
        }

        String name = args[0];

        Configuration conf = new Configuration();
        conf.setInt("rowM", Integer.parseInt(args[3]));
        conf.setInt("columnM", Integer.parseInt(args[4]));
        conf.setInt("columnN", Integer.parseInt(args[5]));

        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MRMulti.class);

        job.setMapperClass(MMapper.class);
        job.setReducerClass(MReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
