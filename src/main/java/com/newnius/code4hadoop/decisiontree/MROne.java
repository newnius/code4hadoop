package com.newnius.code4hadoop.decisiontree;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by newnius on 8/23/17.
 *
 */
public class MROne extends Mapper<Object, Text, Text, Text> {
    private Map<String, Integer> t = new HashMap<>();
    private List<String> records = new ArrayList<>();
    private Set<String> values = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /* init records */
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String sample = records.get(0);
        int count = sample.split("\t").length - 2;
        for(int i=0; i<count; i++) {// for each property
            context.write(new Text(i + ""), new Text("D\t" + records.size()));

            /* emit Dj of each class */
            for (String record : records) {
                String[] line = record.split("\t");
                if (!t.containsKey(line[count + 1])) {
                    t.put(line[count + 1], 1);
                }
                int val = t.get(line[count + 1]);
                val++;
                t.put(line[count + 1], val);
            }
            for (Map.Entry<String, Integer> entry : t.entrySet()) {
                context.write(new Text(i + ""), new Text("D_class_" + entry.getKey() + "\t" + entry.getValue()));
            }
            t.clear();

            /* emit Dj of each value of property i */
            for (String record : records) {
                String[] line = record.split("\t");
                if (!t.containsKey(line[i + 1])) {
                    t.put(line[i + 1], 1);
                    values.add(line[i] + 1);
                }
                int val = t.get(line[i + 1]);
                val++;
                t.put(line[i + 1], val);
            }
            for (Map.Entry<String, Integer> entry : t.entrySet()) {
                context.write(new Text(i + ""), new Text("D_val_" + entry.getKey() + "\t" + entry.getValue()));
            }
            t.clear();

            for (String val : values) {
                for (String record : records) {
                    String[] line = record.split("\t");
                    if(!line[i+1].equals(val)){
                        continue;
                    }
                    if (!t.containsKey(line[count + 1])) {
                        t.put(line[count + 1], 1);
                    }
                    int v = t.get(line[count + 1]);
                    v++;
                    t.put(line[count + 1], v);
                }
                for (Map.Entry<String, Integer> entry : t.entrySet()) {
                    context.write(new Text(i + ""), new Text("D_val_" + val + "_class_" + entry.getKey() + "\t" + entry.getValue()));
                }
                t.clear();
            }
        }

    }
}
