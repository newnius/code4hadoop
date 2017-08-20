package com.newnius.code4hadoop.kmeans;

import java.io.IOException;

/**
 * Created by newnius on 8/20/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.kmeans.Main kmeans/input kmeans/output kmeans/clusters 5
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 4) {
            System.err.println("Usage: <input path> <output path> <cluster_file_path> <loop cnt>");
            System.exit(-1);
        }

        int count = Integer.parseInt(args[3]);

        String[] tmp = {"", "", ""};
        tmp[0] = args[0];
        tmp[2] = args[2];

        for(int i=1; i<=count; i++) {
            tmp[1] = args[1] + "_loop_" + i;
            MROne.main(tmp);
            tmp[2] = args[1] + "_loop_" + i;
        }

        tmp[1] = args[1];
        MRCluster.main(tmp);
    }
}
