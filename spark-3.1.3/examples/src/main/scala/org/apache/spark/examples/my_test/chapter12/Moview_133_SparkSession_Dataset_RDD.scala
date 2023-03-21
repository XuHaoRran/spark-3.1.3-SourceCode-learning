package org.apache.spark.examples.my_test.chapter12


object Moview_133_SparkSession_Dataset_RDD {
    case class Person(name:String, age:Long)
    def main(args: Array[String]): Unit = {
//        import spark.implicits._
//        import org.apache.spark.sql.functions._

//        val conf = new SparkConf().setMaster("localhost[*]").setAppName("hispark")
//        val spark = SparkSession.builder()
//            .config(conf)
//            .config("spark.sql.warehouse.dir", "D:\\programe\\spark-3.1.3\\spark-3.1.3\\data\\my_warehouse")
//            .enableHiveSupport()
//            .getOrCreate()
//        val persons = spark.read.json("data/kv1.txt")
//        val personDS = persons.as[Person]
//        personDS.show()
//        personDS.printSchema()
//        val personDF = personDS.toDF()



    }

}
