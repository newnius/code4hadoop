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
 *  call hadoop jar hadoop-tutorial.jar ListStatus hdfs://localhost/ hdfs://localhost/user/root
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path[] paths = new Path[args.length];

        for(int i=0;i<paths.length; i++){
            paths[i] = new Path(args[i]);
        }

        FileStatus[] stats = fs.listStatus(paths);

        Path[] listPaths = FileUtil.stat2Paths(stats);

        for(Path p: listPaths){
            System.out.println(p);
        }

    }
}
