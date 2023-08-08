package com.xuhaoran.analyze

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Date


/**
 * @author xuhaoran 
 * @date 2023-04-19 18:59
 */
object UserScoreAPP {
    def main(args: Array[String]): Unit = {
        val startTime = System.currentTimeMillis()
        //1  初始化环境
        val sparkConf: SparkConf = new SparkConf().setAppName("UserScoreAPP") .setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        sparkSession.sql("use taobao")
        val sql =
            s"""
               |select  user_id,
               |        case when recent_rank < 671043 * 1 / 4 then 4
               |             when recent_rank < 671043 * 2 / 4 then 3
               |             when recent_rank < 671043 * 3 / 4 then 2
               |             else 1 end +
               |        case when frequency_rank < 671043 * 1 / 4 then 4
               |             when frequency_rank < 671043 * 2 / 4 then 3
               |             when frequency_rank < 671043 * 3 / 4 then 2
               |             else 1 end
               |            as score
               |from
               |    (
               |        select user_id, recent, dense_rank() over (order by recent asc) as recent_rank,  frequency, dense_rank() over (order by frequency desc) as frequency_rank
               |        from (
               |                 select user_id, datediff('2017-12-04', max(create_time)) as recent, count(behavior) frequency
               |                 from dwd_user_behavior
               |                 where behavior='buy'
               |                 group by user_id
               |             ) t1
               |    ) rfm
               |""".stripMargin
//        sparkSession.sql(sql).explain("extended")
        // 查看 RDD 的执行计划
//        val rdd = sparkSession.sql(sql).rdd
//        println(rdd.toDebugString)

        // 调用 RDD 的算子进行计算
//        rdd.foreach(println)

        sparkSession.sql(sql).show()
        val endTime = System.currentTimeMillis()
        val elapsedTime = (endTime - startTime) / 1000.0
        println(s"Execution time: ${elapsedTime}s")

    }

}
