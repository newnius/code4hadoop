package com.newnius.code4hadoop.invertedindex;

import java.io.IOException;

/**
 * Created by newnius on 8/20/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.invertedindex.MainSort invertedindex/input invertedindex/output2
 */
public class MainSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }

        String[] tmp = {"", "", ""};
        tmp[0] = "mr-inverted-index-wc-1";
        tmp[1] = args[0];
        tmp[2] = args[1] + "_tmp";
        MRThree.main(tmp);

        tmp[0] = "mr-inverted-index-wc-2";
        tmp[1] = tmp[2];
        tmp[2] = args[1];
        MRFour.main(tmp);
    }
}
