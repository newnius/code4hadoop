package com.newnius.code4hadoop.book.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by newnius on 12/7/16.
 *
 */
public class ShowFileStatusTest {
    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        if(System.getProperty("test.build.data") == null){
            System.setProperty("test.build.data", "/tmp");
        }
        fs = FileSystem.get(URI.create(""), conf);
        OutputStream out = fs.create(new Path("/dir/file.txt"));
        out.write("content".getBytes("UTF-8"));
        out.close();
    }

    @After
    public void tearDown() throws IOException {
        if(fs != null){
            fs.close();
        }
    }

    @Test
    public void throwsFileNotFoundForNonExistentFile() throws IOException{
        fs.getFileStatus(new Path("no-such-file"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file.txt");
        FileStatus stat = fs.getFileStatus(file);

        assert stat.getPath().toUri().getPath().equals("/dir/file.txt");
        assert !stat.isDirectory();
        assert  stat.getLen() == 7L;


    }

}
