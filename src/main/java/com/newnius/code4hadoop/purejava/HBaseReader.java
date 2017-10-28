package com.newnius.code4hadoop.purejava;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseReader {
    public static void main(String[] args){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper_node1,zookeeper_node2,zookeeper_node3");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "hadoop-master:60000");

        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("Wuxia"));

            Scan scan = new Scan();
            scan.setCaching(20);
            scan.addFamily(Bytes.toBytes("cf"));

            ResultScanner scanner = table.getScanner(scan);

            BufferedWriter bw = new BufferedWriter(new FileWriter("/share/word_occurrence.txt"));

            for (Result next : scanner) {
                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet()) {
                    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet()) {
                        for (Map.Entry<Long, byte[]> entry : entryVersion.getValue().entrySet()) {
                            String row = Bytes.toString(next.getRow());
                            String column = Bytes.toString(entryVersion.getKey());
                            byte[] value = entry.getValue();
                            long timesstamp = entry.getKey();
                            bw.write(row+"\t" + ByteBuffer.wrap(value).getFloat() + "\n");
                        }
                    }
                }
            }

            bw.close();


        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            try {
                if (table != null) table.close();
                if (connection != null) connection.close();
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
