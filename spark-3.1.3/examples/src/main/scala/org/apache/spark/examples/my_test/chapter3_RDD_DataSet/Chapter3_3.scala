package org.apache.spark.examples.my_test.chapter3_RDD_DataSet

import org.apache.spark.{SparkConf, SparkContext}

object Chapter3_3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf() //创建SparkConf对象
        conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
        conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群

        val sc = new SparkContext(conf)

        val num = Array(100,80,70)
        val rddnum = sc.parallelize(num)
        val mapRdd = rddnum.map(_*2)
        mapRdd.collect().foreach(println)

        val data1 = Array("spark","scala","hadoop")
        val data2 = Array("SPARK","SCALA","HADOOP")
        val rdd1 = sc.parallelize(data1)
        val rdd2 = sc.parallelize(data2)
        val unionRdd = rdd1.union(rdd2)
        unionRdd.collect().foreach(println)

    }

}
