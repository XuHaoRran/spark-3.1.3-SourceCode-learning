package org.apache.spark.examples.my_test.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Movie_Users_Analyzer_RDD {
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

        val usersBasic = userRDD.map(_.split("::")).map(
//            UserID::Gender::Age::Occupation::Zip-code
            user => (user(3), (user(0), user(1), user(2)))
        )
        usersBasic.collect().take(2).foreach(a => println("userBasicRDD(职业ID，（用户ID，性别，年龄）):" + a))

        val occupations = occupationsRDD.map(_.split("::")).map(
            occup => (occup(0), occup(1))
        )

        val userInformation = usersBasic.join(occupations)
        userInformation.cache()

        userInformation.collect().take(2).foreach(a => println("userInformation(职业ID，（用户ID，性别，年龄），职业):" + a))

        val targetMovie = ratingsRDD.map(_.split("::")).map(x=>(x(0),x(1))).filter(_._2.equals("1193"))

        val targetUsers = userInformation.map(x=>(x._2._1._1, x._2))

        val userInformationForSpecificMoive = targetUsers.join(targetMovie)

        userInformationForSpecificMoive.collect().take(2).foreach(a => println("userInformationForSpecificMoive(用户ID,(职业ID，（用户ID，性别，年龄），职业),电影):" + a))


    }

}
