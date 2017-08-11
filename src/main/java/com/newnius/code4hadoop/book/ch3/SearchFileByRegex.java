package com.newnius.code4hadoop.book.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by newnius on 12/7/16.
 * call: ./hadoop jar hadoop-tutorial-1.0-SNAPSHOT.jar SearchFileByRegex hdfs:/// /user/root/* /_SUCCESS ^.* /output0.*$
 */
public class SearchFileByRegex {
    public static void main(String args[]) throws IOException {
        if(args.length!=3){
            System.err.println("Usage: uri regex excludedRegex");
        }
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        System.out.println("regexExcluded:");
        FileStatus[] stats = fs.globStatus(new Path(args[1]));

        Path[] listPaths = FileUtil.stat2Paths(stats);
        for(Path p: listPaths){
            System.out.println(p);
        }

        System.out.println("regex:");
        stats = fs.globStatus(new Path(args[1]), new RegexExcludePathFilter(args[2]));
        listPaths = FileUtil.stat2Paths(stats);
        for(Path p: listPaths){
            System.out.println(p);
        }
    }
}
