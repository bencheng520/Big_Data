package com.ben.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @Author 001289
 * @Date 2018/5/4 15:40
 * @Description ${DESCRIPTION}
 */
public class HdfsFileReader {

    static void printAndExit(String str) {
        System.err.println(str);
        System.exit(1);
    }

    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(Config.URI), conf);

        // Hadoop DFS deals with Path
        Path inFile = new Path(Config.URI);

        // Check if input is valid
        if (!fs.exists(inFile)) {
            printAndExit("Input file not found");
        }
        if (!fs.isFile(inFile)) {
            printAndExit("Input should be a file");
        }

        // Read from the file
        FSDataInputStream in = fs.open(inFile);
        byte buffer[] = new byte[256];
        StringBuilder sb = new StringBuilder();
        try {
            while (in.read(buffer) != -1) {
                sb.append(new String(buffer));
            }
            System.out.println(sb.toString());
            System.out.println("reading " + Config.URI + " done");
        } finally {
            in.close();
        }
    }

}
