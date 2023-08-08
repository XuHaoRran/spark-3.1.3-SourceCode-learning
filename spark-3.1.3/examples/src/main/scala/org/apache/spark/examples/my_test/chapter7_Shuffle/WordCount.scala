package org.apache.spark.examples.my_test.chapter7_Shuffle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.INFO)
        val conf = new SparkConf()
        conf.setAppName("FIRST APP")
            .setMaster("local")
//            .set("spark.shuffle.compress", "false")
//            .set("spark.rdd.compress", "false")
//            .set("spark.io.compression.codec", "none")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/wordcount/helloSpark.txt", 2)

        val words = lines.flatMap(line=>line.split(" ")).setName("flatMap操作")

        val pairs = words.map(word => (word, 1)).setName("words.map(word => (word, 1))操作")
        println("操作："+pairs.id + ":" + pairs.name)
        val store = pairs.persist(StorageLevel.MEMORY_AND_DISK).setName("persist存储")
        println("操作："+store.id + ":" + store.name)

        val wordCountOdered = {
            val reducedPairs = pairs.reduceByKey(_+_).setName("reduceByKey操作")
            println("操作："+reducedPairs.id + ":" + reducedPairs.name)

            val fisrtMap = reducedPairs.map(pair=>(pair._2, pair._1)).setName("第一个map(pair=>(pair._2, pair._1))操作")
            println("操作："+fisrtMap.id + ":" + fisrtMap.name)

            val sorted = fisrtMap.sortByKey(false).setName(".sortByKey(false)操作")
            println("操作："+sorted.id + ":" + sorted.name)

            val secondMap = sorted.map(pair=>(pair._2, pair._1)).setName(".第二个map(pair=>(pair._2, pair._1))操作")
            println("操作："+secondMap.id + ":" + secondMap.name)

            secondMap
        }

        wordCountOdered.collect().foreach(numPair=>println(numPair._1 + ":" + numPair._2))
//        while (true){
//            Thread.sleep(1000)
//        }
        sc.stop()
    }

}
