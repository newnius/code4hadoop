package com.newnius.code4hadoop.naivebayes;

import java.io.IOException;

/**
 * Created by newnius on 8/22/17.
 *
 * bin/hadoop jar code4hadoop-1.0.jar com.newnius.code4hadoop.naivebayes.Main naivebayes/input/test.txt naivebayes/input/train.txt naivebayes/output
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: <test file path> <train_file_path> <output path>");
            System.exit(-1);
        }
        String tmp[] = {"", ""};
        tmp[0] = args[1];
        tmp[1] = args[2] + "_trained";
        MRTrain.main(tmp);

        String[] tmp2 = {"", "", ""};
        tmp2[0] = args[0];
        tmp2[1] = args[2];
        tmp2[2] = tmp[1];
        MRTest.main(tmp2);
    }
}
