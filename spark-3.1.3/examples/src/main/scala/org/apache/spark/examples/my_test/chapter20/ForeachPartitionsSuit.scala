package org.apache.spark.examples.my_test.chapter20

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
//import org.datanucleus.store.rdbms.connectionpool.ConnectionPool



/**
 * 如果使用foreach算子，将在每个RDD的每条记录中都进行Connection的建立和关闭，
 * 这会导致不必要的高负荷，并且会降低整个系统的吞吐量，
 * 所以一个更好的方式是使用RDD.foreachPartition，即对于每个RDD的Partition，
 * 建立唯一的连接（每个Partition的RDD是运行在同一个worker上的），
 * 降低了频繁建立连接的负载，通常我们在链接数据库时会使用连接池
 * 。
 */
object ForeachPartitionsSuit {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.INFO)

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]").setAppName("app")
        val sc = new SparkContext(sparkConf)

        /**
        val a = sc.parallelize(1 to 10, 3)
        a.foreachPartition(PartitionOfReacords => {
            val Connection = ConnectionPool.getConnection()
            PartitionOfReacords.foreach(record => Connection.send(record))
            ConnectionPool.returnConnection(Connection) // 返回连接池，用于连接的复用
        })
        **/
    }

}
