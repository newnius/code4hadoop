package com.newnius.code4hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by newnius on 8/17/17.
 *
 */
public class ReducerSix extends Reducer<Text, IntWritable, Text, Text> {
    private int sum = 0;
    private String lastWord = null;
    private HashMap<String, Integer> cnt = new HashMap<>();
    private Table table;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper_node1,zookeeper_node2,zookeeper_node3");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "hadoop-master:60000");

        Connection connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("Wuxia"));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] pair = key.toString().split(",");
        String word = pair[0];

        if(lastWord!=null && !word.equals(lastWord)){
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String, Integer> entry : cnt.entrySet()){
                sb.append(entry.getKey().split(",")[1].replace(".txt.segmented", "").replace(".TXT.segmented", "")).append(":").append(entry.getValue()).append(";");
            }
            context.write(new Text(lastWord), new Text(sb.toString().substring(0, sb.length()-1)));

            try{
                Put p = new Put(Bytes.toBytes(lastWord));
                p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes((float)sum / cnt.size()));
                table.put(p);
            }catch (Exception ex){
                ex.printStackTrace();
            }

            sum = 0;
            cnt.clear();
        }

        for(IntWritable iw: values) {
            Integer t = cnt.get(key.toString());
            if (t == null) {
                t = 0;
            }
            cnt.put(key.toString(), t + iw.get());
            sum += iw.get();
        }

        lastWord = word;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, Integer> entry : cnt.entrySet()){
            sb.append(entry.getKey().split(",")[1].replace(".txt.segmented", "").replace(".TXT.segmented", "")).append(":").append(entry.getValue()).append(";");
        }
        context.write(new Text(lastWord), new Text(sb.toString().substring(0, sb.length()-1)));

        try{
            Put p = new Put(Bytes.toBytes(lastWord));
            p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes((float)sum / cnt.size()));
            table.put(p);
        }catch (Exception ex){
            ex.printStackTrace();
        }

        if (table != null) table.close();
    }
}
