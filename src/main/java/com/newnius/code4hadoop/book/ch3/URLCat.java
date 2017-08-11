package com.newnius.code4hadoop.book.ch3;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by newnius on 12/7/16.
 * call: hadoop jar hadoop-tutorial.jar URLCat hdfs://localhost/user/root/input.txt
 */
public class URLCat {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    
    public static void main(String[] args) throws IOException {
        InputStream in = null;
        try{
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }

    }
}
