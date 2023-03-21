package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object Movie_127_SparkSQL_RDD {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
        conf.setAppName("FIRST APP")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)

        val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

        val dataPath = "data/ml-1m/"

        val userRDD = sc.textFile(dataPath + "users.dat")
        val moviesRDD = sc.textFile(dataPath + "movies.dat")
        val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
        val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
//        1::1193::5::978300760
//        UserID::MovieID::Rating::Timestamp
        val schemaforusers = StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")
            .map(column => StructField(column, StringType, true)))
        val userRDDRows = userRDD.map(_.split("::")).map(
            line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim)
        )
        val userDataFrame = spark.createDataFrame(userRDDRows, schemaforusers)

        val schemaforratings = StructType("UserID::MovieID".split("::")
            .map(column => StructField(column, StringType, true)))
            .add("Rating", DoubleType, true)
            .add("Timestamp", StringType, true)

        val ratingsRDDRows = ratingsRDD.map(_.split("::"))
            .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
        val ratingDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

        val schemaformovies = StructType("MovieID::Title::Genres".split("::")
            .map(column => StructField(column, StringType, true)))
        val moviesRDDRows =  moviesRDD.map(_.split("::"))
            .map(line => Row(line(0).trim, line(1).trim, line(2).trim))
        val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)

        ratingDataFrame.filter(s"MovieID = 1193")
            .join(userDataFrame, "UserID")
            .select("Gender","Age")
            .groupBy("Gender", "Age")
            .count()
            .show(10)

        println("功能二：用GlobalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人")
        ratingDataFrame.createGlobalTempView("ratings")
        userDataFrame.createGlobalTempView("users")

        spark.sql("select gender, age, count(*) from global_temp.users u " +
            "join global_temp.ratings as r on u.UserID = r.UserID where MovieID = 1193 " +
            "group by Gender, Age" +
            "").show(10)

        println("功能二：用LocalTempView的SQL语句实现某特定电影观看者中男性和女性不同年龄分别有多少人")
        ratingDataFrame.createTempView("ratings")
        userDataFrame.createTempView("users")

        spark.sql(
            """select gender, age, count(*) from users u join ratings as r
              | on u.UserId = r.UserID where MovieID = 1193
              | group by Gender, Age
              |""".stripMargin).show(10)
    }

}
