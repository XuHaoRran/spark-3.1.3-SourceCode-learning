package org.apache.spark.examples.my_test.chapter3_RDD_DataSet

import org.apache.spark.{SparkConf, SparkContext}

object Chapter3_7 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf() //创建SparkConf对象
        conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
        conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群

        val sc = new SparkContext(conf)
        sc.textFile("D:\\programe\\spark-3.1.3\\spark-3.1.3\\examples\\src\\main\\scala\\org\\apache\\spark\\examples\\my_test\\chapter3_RDD_DataSet\\helloSpark.txt").flatMap(_.split(" "))
            .map(word=> (word, 1)).reduceByKey(_+_).saveAsTextFile("D:\\programe\\spark-3.1.3\\spark-3.1.3\\examples\\src\\main\\scala\\org\\apache\\spark\\examples\\my_test\\chapter3_RDD_DataSet\\test")

    }

}
