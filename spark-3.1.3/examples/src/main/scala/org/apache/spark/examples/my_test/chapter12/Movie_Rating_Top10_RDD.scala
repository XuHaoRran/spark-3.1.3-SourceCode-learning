package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Movie_Rating_Top10_RDD {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
        conf.setAppName("FIRST APP")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)

        val dataPath = "data/ml-1m/"

        val userRDD = sc.textFile(dataPath + "users.dat")
        val moviesRDD = sc.textFile(dataPath + "movies.dat")
        val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
        val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
//        1::1193::5::978300760
//        UserID::MovieID::Rating::Timestamp
        val rating = ratingsRDD.map(_.split("::")).map(
            // 电影ID， 评分数据， 计数
            a => (a(1), (a(2).toInt, 1))
        )

        val sum = rating.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        sum.collect().foreach(println)

        sum.map(a => ((a._2._1.toDouble / a._2._2), a._1)).sortByKey(false).take(10).foreach(println)



    }

}
