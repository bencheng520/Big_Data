package com.ben.hadoop.spark

import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * @Author 001289
  * @Date 2018/5/4 16:45
  * @Description ${DESCRIPTION}
  */
object SparkScalaMysqlToHDFS {
  def main(args: Array[String]) {
    val inputFile = "file:///usr/local/spark/mycode/wordcount/word.txt"
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
