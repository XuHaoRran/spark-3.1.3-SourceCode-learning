package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Movie_Gender_Rating_Top10_RDD {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
        conf.setAppName("FIRST APP")
        conf.setMaster("local")
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
            a => (a(0), (a(1), a(2).toInt))
        )
        val user = userRDD.map(_.split("::")).map(
            a => (a(0), a(1))
        )
        val joined = user.join(rating)

        val male = joined.filter(a=> a._2._1.equals("M"))
        val female = joined.filter(a=> a._2._1.equals("F"))
        male.map(
            a => ((a._2._1, a._2._2._1), (a._2._2._2, 1))
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
            .map(a => ((a._2._1 / a._2._2), a._1 ))
            .sortByKey(false)
            .collect()
            .take(10)
            .foreach(println)
        female.map(
            a => ((a._2._1, a._2._2._1), (a._2._2._2, 1))
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
            .map(a => ((a._2._1 / a._2._2), a._1))
            .sortByKey(false)
            .collect()
            .take(10)
            .foreach(println)
    }

}
