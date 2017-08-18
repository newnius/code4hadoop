package com.newnius.code4hadoop.pagerank;

import java.io.IOException;

/**
 * Created by newnius on 8/18/17.
 *
 * bin/hadoop jar /mnt/jars/hadoop-tutorial-1.0-SNAPSHOT.jar com.newnius.code4hadoop.pagerank.Main pagerank/input pagerank/output 5
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <input path> <output path> <loop cnt>");
            System.exit(-1);
        }

        int count = Integer.parseInt(args[2]);

        String[] tmp = {"", ""};
        tmp[0] = args[0];

        for(int i=1; i<=count; i++){
            tmp[1] = args[0] + "_output_" + i;
            MRCal.main(tmp);
            tmp[0] = args[0] + "_output_" + i;
        }

        tmp[1] = args[1];
        MRSorter.main(tmp);
    }
}
