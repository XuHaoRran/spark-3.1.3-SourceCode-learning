package org.apache.spark.examples.my_test.chapter20

import breeze.numerics.sqrt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.random.RandomRDDs.normalRDD
//import org.apache.spark.mllib.random.RandomRDDs.normalRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 如果使用foreach算子，将在每个RDD的每条记录中都进行Connection的建立和关闭，
 * 这会导致不必要的高负荷，并且会降低整个系统的吞吐量，
 * 所以一个更好的方式是使用RDD.foreachPartition，即对于每个RDD的Partition，
 * 建立唯一的连接（每个Partition的RDD是运行在同一个worker上的），
 * 降低了频繁建立连接的负载，通常我们在链接数据库时会使用连接池
 * 。
 */
object TreeAggregateSuit {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.INFO)

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]").setAppName("app")
        val sc = new SparkContext(sparkConf)

        var mapReduceTimeArr : Array[Double] = Array.ofDim(20)
        var treeReduceTimeArr : Array[Double] = Array.ofDim(20)
        var treeAggregateTimeArr : Array[Double] = Array.ofDim(20)

        val sql : SQLContext = new SQLContext(sc)

//         生成一个随机的RDD,包含100万哥来自标准正太分布，
//         均匀分布在5个分区中
        val input1 = normalRDD(sc, 100000L, 5)

        // 生成一个随机的RDD，包含100万个来自标准正太分布的独立同分布的值
        // 均匀分布在5个分区中
        val input2 = normalRDD(sc, 100000L, 5)

        val xy = input1.zip(input2).cache()

        xy.count()

        for(i:Int <- 0 until 20){
            val t1 = System.nanoTime()
            val euclideanDistanceMapRed = sqrt(xy.map{
                    case (v1, v2) => (v1 - v2) * (v1 - v2)
                }.reduce(_+_)
            )
            val t11 = System.nanoTime()
            println("Map Reduce - Euclidean Distance " + euclideanDistanceMapRed)
            mapReduceTimeArr(i) = (t11 - t1) / 100000L
            println("Map Reduce - Elapsed time: " + mapReduceTimeArr(i) + "ms")
        }

        for (i: Int <- 0 until 20) {
            val t2 = System.nanoTime()
            val euclideanDistanceMapRed = sqrt(xy.map {
                case (v1, v2) => (v1 - v2) * (v1 - v2)
            }.treeReduce(_ + _))
            val t22 = System.nanoTime()
            println("Tree Reduce - Euclidean Distance " + euclideanDistanceMapRed)
            treeReduceTimeArr(i) = (t22 - t2) / 100000L
            println("Tree Reduce - Elapsed time: " + treeReduceTimeArr(i) + "ms")
        }

        for (i: Int <- 0 until 20) {
            val t3 = System.nanoTime()
            val euclideanDistanceMapRed = sqrt(xy.treeAggregate(0.0)(
                seqOp = (c,v) => {
                    (c + ((v._1 - v._2))) * (v._1 - v._2)
                },
                combOp = (c1, c2) => {
                    (c1 + c2)
                }
            ))
            val t33 = System.nanoTime()
            println("Tree Aggregate - Euclidean Distance " + euclideanDistanceMapRed)
            treeAggregateTimeArr(i) = (t33 - t3) / 100000L
            println("Tree Aggregate - Elapsed time: " + treeAggregateTimeArr(i) + "ms")
        }

        val mapReduceAvgTime = mapReduceTimeArr.sum / mapReduceTimeArr.length
        val treeReduceAvgTime = treeReduceTimeArr.sum / treeReduceTimeArr.length
        val treeAggregateTime = treeAggregateTimeArr.sum / treeAggregateTimeArr.length

        val mapReduceMinTime = mapReduceTimeArr.min
        val treeReduceMinTime = mapReduceTimeArr.min
        val treeAggregateMinTime = treeAggregateTimeArr.min

        val mapReduceMaxTime = mapReduceTimeArr.max
        val treeReduceMaxTime = mapReduceTimeArr.max
        val treeAggregateMaxTime = treeAggregateTimeArr.max

        println("Map-Reduce - AVG:" + mapReduceAvgTime + "ms  MAX:" + mapReduceMaxTime + "ms   MIN:" + mapReduceMinTime + "ms")
        println("Tree-Reduce - AVG:" + treeReduceAvgTime + "ms  MAX:" + treeReduceMaxTime + "ms   MIN:" + treeReduceMinTime + "ms")
        println("Tree-Aggregate - AVG:" + treeAggregateTime + "ms  MAX:" + treeAggregateMaxTime + "ms   MIN:" + treeAggregateMinTime + "ms")


    }

}
