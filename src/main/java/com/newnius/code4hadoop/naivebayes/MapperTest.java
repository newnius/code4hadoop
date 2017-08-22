package com.newnius.code4hadoop.naivebayes;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by newnius on 8/22/17.
 *
 */
public class MapperTest extends Mapper<Object, Text, Text, Text> {
    private Map<String, Integer> freq = new HashMap<>();
    private Map<String, Integer> count = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        for(URI uri: uris){
            String line;
            BufferedReader br = new BufferedReader(new FileReader(new Path(uri).getName()));
            while((line = br.readLine()) != null){
                String[] t = line.split("\t");
                if(t[0].contains("#")){
                    freq.put(t[0], Integer.parseInt(t[1]));
                }else{
                    count.put(t[0], Integer.parseInt(t[1]));
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        double max = -1;
        String clazz = "";
        String[] line = value.toString().split("\t");
        for(String c: count.keySet()){
            double f = 1;
            f *= count.get(c);
            for(int j=1; j<line.length; j++){
                if(freq.get(c + "#col_" + j + "#" + line[j])==null){
                    f = 0;
                }else{
                    f *= freq.get(c + "#col_" + j + "#" + line[j]) / count.get(c);
                }
            }
            if(f > max){
                max = f;
                clazz = c;
            }
        }
        context.write(new Text(line[0]), new Text(clazz+"\t"+max));
    }
}
