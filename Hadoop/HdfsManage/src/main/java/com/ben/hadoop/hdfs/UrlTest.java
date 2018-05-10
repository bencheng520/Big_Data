package com.ben.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * @Author 001289
 * @Date 2018/5/4 15:41
 * @Description ${DESCRIPTION}
 */
public class UrlTest {

    /**
     * 请注意  原生的hadoop hdfs端口是9000   cdh版本的hadoop hdfs是8020
     */
    private static final String HDFS_URL="hdfs://localhost:9000";

    private static final String DIR_URL="/zhuo";

    private static final String FILE_URL="/zhuo/yuan";

    private static final String FILE1_URL="/ming";

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new URI(HDFS_URL),new Configuration());

//      createDir(fs);
//      upfile(fs);

//       downFile(fs);

//      deleteFIle(fs);

        deleteDir(fs);
    }



    private static void deleteDir(FileSystem fs) throws IOException {
        //删除文件夹
        boolean g = fs.delete(new Path(DIR_URL),true);
        System.out.println(g);
    }



    private static void deleteFIle(FileSystem fs) throws IOException {
        //删除文件
        boolean b = fs.delete(new Path(FILE1_URL), true);
        System.out.println(b);
    }



    private static void downFile(FileSystem fs) throws IOException {
        //下载文件
        FSDataInputStream fis = fs.open(new Path(FILE_URL));
        IOUtils.copyBytes(fis, System.out,1024,true);
    }



    private static void createDir(FileSystem fs) throws IOException {
        //创建文件夹
        fs.mkdirs(new Path(DIR_URL));
    }



    private static void upfile(FileSystem fs) throws FileNotFoundException, IOException {
        FileInputStream in=new FileInputStream("yuan.txt");
        //上传文件
        FSDataOutputStream fos = fs.create(new Path(FILE_URL),true);
        IOUtils.copyBytes(in, fos, 1024,true);
    }
}
