package com.ben.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Author 001289
 * @Date 2018/5/4 15:25
 * @Description ${DESCRIPTION}
 */
public class HdfsFileWriter {
    static void printAndExit(String str) {
        System.err.println(str);
        System.exit(1);
    }

    public static void main(String[] argv) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(Config.URI), conf);

        // Hadoop DFS deals with Path
        Path outFile = new Path(Config.URI);

        // Check if output is valid
        if (fs.exists(outFile)) {
            printAndExit("Output already exists");
        }

        // Write to the new file
        String text = "Hello from HdfsFileWriter!";
        InputStream in = new ByteArrayInputStream(text.getBytes());
        FSDataOutputStream out = fs.create(outFile);
        byte buffer[] = new byte[256];
        try {
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            System.out.println("writing " + Config.URI + " done");
        } catch (IOException e) {
            System.out.println("Error while writing file");
        } finally {
            out.close();
            fs.close();
        }
    }
}
