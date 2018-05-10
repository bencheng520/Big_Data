package com.ben.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author 001289
 * @Date 2018/5/8 20:09
 * @Description ${DESCRIPTION}
 */
public class WordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("test-for-spark-ui");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //知识，哪怕是知识的幻影，也会成为你的铠甲，保护你不被愚昧反噬。
        JavaPairRDD<String,Integer> counts = sc.textFile( "C:\\Users\\xinghailong\\Desktop\\你为什么要读书.txt" )
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(s -> new Tuple2<String,Integer>(s,new Integer(1)))
                .reduceByKey((x,y) -> x+y);

        counts.cache();
        List<Tuple2<String,Integer>> result = counts.collect();
        for(Tuple2<String,Integer> t2 : result){
            System.out.println(t2._1+" : "+t2._2);
        }
        sc.stop();
    }

}
