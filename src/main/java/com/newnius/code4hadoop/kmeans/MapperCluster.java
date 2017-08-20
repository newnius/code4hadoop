package com.newnius.code4hadoop.kmeans;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class MapperCluster extends Mapper<Object, Text, Text, Text> {
    private List<Cluster> clusters = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("cluster_file");
        FileStatus[] files = fs.listStatus(new Path(filename));
        String line;
        for (FileStatus file : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            while ((line = br.readLine()) != null) {
                Cluster cluster = new Cluster();
                cluster.fromString(line.substring(line.indexOf("\t") + 1));
                clusters.add(cluster);
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        double x = Double.parseDouble(line[0]);
        double y = Double.parseDouble(line[1]);
        Double distance = null;
        Cluster c = null;
        double dis;
        for(Cluster cluster : clusters) {
            dis = (cluster.getX() - x) * (cluster.getX() - x) + (cluster.getY() - y) * (cluster.getY() - y);
            if (distance == null || dis < distance) {
                distance = dis;
                c = cluster;
            }
        }
        if (c != null) {
            context.write(new Text(c.getX() + "," + c.getY()), new Text(x + "," + y));
        }
    }
}
