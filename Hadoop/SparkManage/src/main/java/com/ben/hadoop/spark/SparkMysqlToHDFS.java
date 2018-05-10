package com.ben.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

/**
 * @Author 001289
 * @Date 2018/5/4 16:45
 * @Description ${DESCRIPTION}
 */
public class SparkMysqlToHDFS {

    public static void main( String[] args ){
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysqlToHDFS").setMaster("local[1]"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        readMySQL(sqlContext);
        //停止SparkContext
        sparkContext.stop();
    }

    private static void readMySQL(SQLContext sqlContext){
        String url = "jdbc:mysql://10.204.2460.64:3306/test";
        //查找的表名
        String table = "sys_user";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","root");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");
        // 读取表中所有数据
        Dataset<Row> jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        //显示数据
        jdbcDF.show();
    }

}
