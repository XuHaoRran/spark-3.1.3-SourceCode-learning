package org.apache.spark.examples.my_test.chapter12

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Moview_133_SparkSession_RDD {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("hispark")
        val spark = SparkSession.builder()
            .config(conf)
            .enableHiveSupport()
            .master("local[*]")
            .getOrCreate()
        spark.sql("create table if not exists src(key INT, value STRING)")
        spark.sql("LOAD DATA LOCAL INPATH 'data/kv1.txt' into table src")
        spark.sql("select * from src").show
        spark.sql("selct count(*) from src").show



    }

}
