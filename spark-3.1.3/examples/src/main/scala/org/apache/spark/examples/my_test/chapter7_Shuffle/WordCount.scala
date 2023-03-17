package org.apache.spark.examples.my_test.chapter7_Shuffle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ALL)
        val conf = new SparkConf()
        conf.setAppName("FIRST APP")
        conf.setMaster("local")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/wordcount/helloSpark.txt", 1)

        val words = lines.flatMap(line=>line.split(" "))

        val pairs = words.map(word => (word, 1))

        val wordCountOdered = pairs.reduceByKey(_+_)
            .map(pair=>(pair._2, pair._1)).sortByKey(false)
            .map(pair=>(pair._2, pair._1))

        wordCountOdered.collect().foreach(numPair=>println(numPair._1 + ":" + numPair._2))
        while (true){
            Thread.sleep(1000)
        }
        sc.stop()
    }

}
