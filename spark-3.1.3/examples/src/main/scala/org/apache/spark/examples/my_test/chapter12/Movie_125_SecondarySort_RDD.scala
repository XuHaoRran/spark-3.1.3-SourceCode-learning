package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


class SecondarySortKey(val first: Double, val second:Double) extends Ordered[SecondarySortKey] with Serializable {
    override def compare(other: SecondarySortKey): Int = {
        if (this.first - other.first != 0) {
            (this.first - other.first).toInt
        }else {
            if (this.second - other.second > 0) {
                Math.ceil(this.second - other.second).toInt
            } else if(this.second - other.second < 0){
                Math.floor(this.second - other.second).toInt
            } else {
                (this.second - other.second).toInt
            }
        }
    }
}
object Movie_125_SecondarySort_RDD {

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

        println("对电影评分数据以Timestamp和Rating两个维度进行二次降序排列")
        val pairWithSortkey = ratingsRDD.map(line=>{
            val splited = line.split("::")
            (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
        })
        val sorted = pairWithSortkey.sortByKey(false)

        val sortedResult = sorted.map(sortedline => sortedline._2)
        sortedResult.take(10).foreach(println)
    }

}
