package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/17/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.invertedindex.MRThree InvertedIndex inverted/input inverted/output2
 */
public class MRThree {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <mr name> <input path> <output path>");
            System.exit(-1);
        }

        String name = args[0];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MRThree.class);

        //job.addCacheFile(new Path("inverted/skipwords.txt").toUri());

        job.setMapperClass(MapperTwo.class);
        job.setReducerClass(ReducerTwo.class);
        job.setCombinerClass(CombinerTwo.class);
        job.setPartitionerClass(PartitionerTwo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
