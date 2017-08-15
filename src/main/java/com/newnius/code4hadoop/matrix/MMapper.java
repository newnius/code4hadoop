package com.newnius.code4hadoop.matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by newnius on 8/15/17.
 *
 */
public class MMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int rowM = context.getConfiguration().getInt("rowM", 0);
        int columnM = context.getConfiguration().getInt("columnM", 0);
        int columnN = context.getConfiguration().getInt("columnN", 0);
        String Matrix = ((FileSplit)context.getInputSplit()).getPath().getName();
        if(Matrix.contains("Matrix_M")){
            Matrix = "M";
        }else{
            Matrix = "N";
        }

        String[] tuples = value.toString().split("\t");
        String[] indexs = tuples[0].split(",");

        if(Matrix.equals("M")) {
            for (int i = 1; i <= columnN; i++) {
                context.write(new Text(indexs[0] + "," + i), new Text(Matrix + "," + indexs[1] + "," + tuples[1]));
            }
        }else{
            for (int i = 1; i <= rowM; i++) {
                context.write(new Text(i + "," + indexs[1]), new Text(Matrix + "," + indexs[0] + "," + tuples[1]));
            }
        }
    }
}
