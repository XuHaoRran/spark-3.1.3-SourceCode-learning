package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

object Movie_124_Top10_RDD {
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
        val targetQQUsers = userRDD.map(_.split("::"))
            .map(x=>(x(0),x(2))).filter(_._2.equals("18"))
        val targetTaobaoUsers = userRDD.map(_.split("::"))
            .map(x => (x(0), x(2))).filter(_._2.equals("25"))
        val targetQQUserSet = HashSet() ++ targetQQUsers.map(_._1).collect()
        val targetTaobaoUserSet = HashSet() ++ targetTaobaoUsers.map(_._1).collect()

        val targetQQUsersBroadcast = sc.broadcast(targetQQUserSet)
        val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUserSet)

        val moveID2Name = moviesRDD.map(_.split("::")).map(x=>(x(0),x(1))).collect().toMap
        println("所有电影中QQ或微信核心用户最喜爱电影TopN分析")
        ratingsRDD.map(_.split("::")).map(x=>(x(0), x(1))).filter(x => targetQQUsersBroadcast.value.contains(x._1))
            .map(x=>(x._2, 1)).reduceByKey(_ + _).map(x => (x._2, x._1))
            .sortByKey(false)
            .map(x => (x._2, x._1))
            .take(10)
            .map(x => (moveID2Name.getOrElse(x._1, null), x._2)).foreach(println)
    }

}
