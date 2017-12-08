package com.newnius.code4hadoop.decisiontree;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by newnius on 8/23/17.
 *
 */
public class ReducerOne extends Reducer<Text, Text, Text, DoubleWritable>{
    private Map<String, Integer> t = new HashMap<>();
    private Set<String> vals = new HashSet<>();
    private Set<String> classes = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int class_count = 10;

        /* calculate infoD */
        double sum = 0;
        for(int i=0;i<class_count;i++){
            double p = t.get("D_class_"+i) / t.get("D");
            sum += (Math.log(p) / Math.log(2)) * p + p;
        }
        double infoD = - sum;

        /* calculate info(property_j)D */
        for(String val: vals){
            sum = 0;
            /* calculate infoDj */
            double sum2 = 0;
            for(String clazz: classes){
                double p = t.get("D_val_"+val+"_class_"+clazz) / t.get("D_val_" + val);
                sum2 += (Math.log(p) / Math.log(2)) * p + p;
            }
            double infoDj = - sum2;

            double p = t.get("D_val_"+val) / t.get("D");
            sum += p * infoDj;
        }
        double infojD = - sum;
        double gainj = infoD - infojD;

        /* calculate SplitInfojD */
        sum = 0;
        for(String val: vals){
            double p = t.get("D_val_"+val) / t.get("D");
            sum += (Math.log(p) / Math.log(2)) * p + p;
        }
        double splitInfojD = - sum;

        double gainRatiojD = gainj / splitInfojD;
        context.write(key, new DoubleWritable(gainRatiojD));
    }
}
