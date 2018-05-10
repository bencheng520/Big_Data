package com.ben.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Hello world!
 *
 */
public class AppMain {

    private static String hdfsServer = "hdfs://10.204.243.37:8020/";
    private static String filePath = "/liaokang/file/file1.txt";

    public static void main( String[] args ) throws IOException {
        System.out.println( "Hello World!" );
        //修改hdfs新建者的名称
        System.setProperty("HADOOP_USER_NAME","appdeploy");
        if(!existsFile(filePath)){
            createFile(filePath);
        }
        writeFile(filePath, "this is a test file!\nthis is a liaokang file!");
        readFile(filePath);
    }

    public static void createFile(String filePath) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsServer),conf);
        FSDataOutputStream hdfsOutStream = fs.create(new Path(filePath));
    //    hdfsOutStream.writeChars("hello");
        hdfsOutStream.close();
        fs.close();
    }

    public static boolean existsFile(String filePath) throws IOException {
        boolean exists;
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsServer),conf);
        Path path = new Path(filePath);
        exists = fs.exists(path);
        fs.close();
        return exists;
    }

    public static void writeFile(String filePath, String content) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsServer),conf);
        FSDataOutputStream hdfsOutStream = fs.append(new Path(filePath));
        hdfsOutStream.writeChars(content);
        hdfsOutStream.close();
        fs.close();
    }

    public static void readFile(String filePath) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsServer),conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(filePath));
        byte[] ioBuffer =new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);
        while(readLen!=-1)
        {
            System.out.write(ioBuffer,0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        hdfsInStream.close();
        fs.close();
    }

}
