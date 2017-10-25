package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class PartitionerFive extends HashPartitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        String word = key.toString().substring(0, key.toString().lastIndexOf(","));
        return super.getPartition(new Text(word), value, numReduceTasks);
    }
}
