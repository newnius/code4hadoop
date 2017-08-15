package com.newnius.code4hadoop.matrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 */
public class MReducer extends Reducer<Text, Text, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int rowM = context.getConfiguration().getInt("rowM", 0);
        int columnM = context.getConfiguration().getInt("columnM", 0);
        int columnN = context.getConfiguration().getInt("columnN", 0);

        int[] M = new int[columnM + 1];
        int[] N = new int[columnM + 1];

        for(Text val: values){
            String[] tuple = val.toString().split(",");
            if(tuple[0].equals("M")){
                M[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
            }else{
                N[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
            }
        }

        int sum = 0;
        for(int i=1; i<=columnM; i++){
            sum += M[i]*N[i];
        }
        context.write(key, new IntWritable(sum));
    }
}
