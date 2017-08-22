package com.newnius.code4hadoop.naivebayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by newnius on 8/22/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.naivebayes.MRTest naivebayes/input naivebayes/output naivebayes/trained
 */
public class MRTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <input path> <output path> <trained_file_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "NaiveBayes");
        job.setJarByClass(MRTest.class);

        job.addCacheFile(new Path(args[2]).toUri());

        job.setMapperClass(MapperTest.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
