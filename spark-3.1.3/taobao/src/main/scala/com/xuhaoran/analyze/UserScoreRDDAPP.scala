package com.xuhaoran.analyze

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Date, Timestamp}



/**
 * @author xuhaoran 
 * @date 2023-04-19 18:59
 */
object UserScoreRDDAPP {
    def main(args: Array[String]): Unit = {
        val startTime = System.currentTimeMillis()
        //1  初始化环境
        val sparkConf: SparkConf = new SparkConf().setAppName("UserScoreAPP").set("spark.default.parallelism", 16+"") //.setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
//        sparkSession.sql("use taobao")
        import sparkSession.implicits._
        // Load the data and filter by 'buy' behavior
        val behaviorRDD = sparkSession.table("taobao.dwd_user_behavior").rdd
            .filter(row => row.getAs[String]("behavior") == "buy" && (row.getAs[String]("create_time") != null))


        // Calculate recent and frequency for each user_id
        val userRF = behaviorRDD
            .map(row => (row.getAs[Long]("user_id"), (Timestamp.valueOf(row.getAs[String]("create_time")), 1)))
            .reduceByKey((a, b) => (if (a._1.after(b._1)) a._1 else b._1, a._2 + b._2))
            .map { case (userId, (maxDate, frequency)) =>
                val recent = (Date.valueOf("2017-12-04").getTime - maxDate.getTime) / (24 * 3600 * 1000)
                (userId, recent, frequency)
            }.cache()

        // Calculate recent_rank and frequency_rank for each user_id
        val sortedRecent = userRF.sortBy(_._2).zipWithIndex.map { case (row, index) => (row._1, index + 1, row._3) }
        val sortedFrequency = userRF.sortBy(-_._3).zipWithIndex.map { case (row, index) => (row._1, index + 1) }

        val userRFM = sortedRecent
            .map { case (userId, recentRank, _) => (userId, recentRank) }
            .join(sortedFrequency)

        // Calculate the final score for each user_id
        val userScoreRDD = userRFM.map { case (userId, (recentRank, frequencyRank)) =>
            val recentScore = if (recentRank < 671043 * 1 / 4) 4 else if (recentRank < 671043 * 2 / 4) 3 else if (recentRank < 671043 * 3 / 4) 2 else 1
            val frequencyScore = if (frequencyRank < 671043 * 1 / 4) 4 else if (frequencyRank < 671043 * 2 / 4) 3 else if (frequencyRank < 671043 * 3 / 4) 2 else 1
            (userId, recentScore + frequencyScore)
        }

        // Perform operations on RDD
        userScoreRDD.take(10).foreach(println)
        val endTime = System.currentTimeMillis()
        val elapsedTime = (endTime - startTime) / 1000.0
        println(s"Execution time: ${elapsedTime}s")

    }

}
