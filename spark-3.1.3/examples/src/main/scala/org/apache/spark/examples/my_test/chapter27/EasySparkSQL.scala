package org.apache.spark.examples.my_test.chapter27
import org.apache.spark.sql.SparkSession
case class Student(name: String, age: Int)

object EasySparkSQL {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("Student")
            .master("local[*]")
            .getOrCreate()

        val sc = spark.sparkContext
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import spark.implicits._

        val df = sc.textFile("data/student.txt").map(line => line.split(","))
            .map(p => Student(p(0), p(1).toInt)).toDF()
        df.createOrReplaceTempView("student")

//        val df = rdd.toDF()
//        df.createOrReplaceTempView("student")

        val result = spark.sql("SELECT name FROM student WHERE age >= 13 AND age <= 19")

        result.collect().foreach(row => println)
    }

}
