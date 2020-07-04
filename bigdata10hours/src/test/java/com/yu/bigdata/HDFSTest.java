package com.yu.bigdata;

import junit.extensions.TestSetup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @Author yu
 * @DateTime 2020/7/4 20:09
 */
public class HDFSTest {

    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static final String HDFS_URI = "hdfs://bigdata001:9000";


    @Before
    public void setUp() throws Exception{
        System.out.println("hdfs setUp!");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_URI), configuration,"bigdata");
    }


    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem.close();
        System.out.println("hdfs tearDown!");
    }


    @Test
    public void testMkdirs() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/hdfsapi/t1"));
    }


    @Test
    public void createTest() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/t1/a.txt"));
        fsDataOutputStream.write("hello hdfs ys".getBytes());
        fsDataOutputStream.close();
    }


    @Test
    public void catTest() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/t1/a.txt"));
//        byte[] bytes = new byte[1024];
//        int len = 0;
//        while ((len=in.read(bytes))!=-1){
//            System.out.write(bytes,0,len);
//            System.out.println();
//        }
        IOUtils.copyBytes(in,System.out,1024);
        in.close();
    }

    @Test
    public void rename() throws Exception{
        fileSystem.rename(new Path("/hdfsapi/t1/a.txt"),new Path("/hdfsapi/t1/a_new.txt"));
    }


    @Test
    public void listFile() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String directory = fileStatus.isDirectory() ? "文件夹":"文件";
            short replication = fileStatus.getReplication();
            String path = fileStatus.getPath().toString();

            System.out.println(directory+"/t"+replication+"/t"+path);
        }
    }

    @Test
    public void deleteTest() throws Exception{
        fileSystem.delete(new Path("/a"),true);
    }
}
