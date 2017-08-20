package com.newnius.code4hadoop.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class ReducerOne extends Reducer<IntWritable, Cluster, IntWritable, Cluster> {
    @Override
    protected void reduce(IntWritable key, Iterable<Cluster> values, Context context) throws IOException, InterruptedException {
        double x = 0.0;
        double y = 0.0;
        int num = 0;
        for (Cluster cluster: values){
            x += cluster.getNum() * cluster.getX();
            y += cluster.getNum() * cluster.getY();
            num += cluster.getNum();
        }
        Cluster cluster = new Cluster();
        cluster.setId(key.get());
        cluster.setNum(num);
        cluster.setX(x / num);
        cluster.setY(y / num);
        context.write(key, cluster);
    }
}
