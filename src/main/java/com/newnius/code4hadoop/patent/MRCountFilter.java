package com.newnius.code4hadoop.patent;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/19/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.patent.MRCountFilter patent/input/apat63_99.txt patent/output_count_filter year
 */
public class MRCountFilter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <input path> <output path> <field>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("field", args[2]);

        Job job = Job.getInstance(conf, "PatentCountFilter");
        job.setJarByClass(MRCountFilter.class);

        job.setMapperClass(MapperCountFilter.class);
        job.setReducerClass(ReducerCiteCountStat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
