package com.newnius.code4hadoop.knn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by newnius on 8/20/17.
 *
 */
public class MapperOne extends Mapper<Object, Text, IntWritable, Text> {
    private List<Node> nodes = new ArrayList<>();
    private int k = 7;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        for(URI uri: uris){
            String line;
            BufferedReader br = new BufferedReader(new FileReader(new Path(uri).getName()));
            while((line = br.readLine()) != null){
                Node node = new Node();
                node.fromString(line);
                nodes.add(node);
            }
        }
        k = context.getConfiguration().getInt("k", 7);
    }


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<Node> knns = new ArrayList<>(k);
        List<Double> distances = new ArrayList<>(k);

        Node t = new Node();
        t.fromString(value.toString());

        for(Node node: nodes){
            double dis = (node.getX() - t.getX()) * (node.getX() - t.getX()) + (node.getY() - t.getY()) + (node.getY() - t.getY());
            if(knns.size() < k){
                knns.add(node);
                distances.add(dis);
            }else{
                int index = -1;
                double max = 0.0;
                for(int i=0; i<distances.size(); i++){
                    if(index == -1 || distances.get(i) > max){
                        index = i;
                        max = distances.get(i);
                    }
                }
                if(dis < max){
                    knns.remove(index);
                    distances.remove(index);
                    knns.add(node);
                    distances.add(dis);
                }
            }
        }

        StringBuilder sb = new StringBuilder("");
        for (Node node: knns){
            sb.append(node.getType()).append(",");
        }
        String s = sb.toString();
        if(s.length() > 0){
            s = s.substring(0, s.length() - 1);
        }
        context.write(new IntWritable(t.getId()), new Text(s));
    }
}
