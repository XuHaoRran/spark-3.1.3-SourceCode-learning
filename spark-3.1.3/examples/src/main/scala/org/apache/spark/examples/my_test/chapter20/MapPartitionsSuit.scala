package org.apache.spark.examples.my_test.chapter20

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从输入函数（myfuncPerElement、myfuncPerPartition）层面看，map是推模式，数据被推到myfuncPerElement中；
 * mapPartitons是拉模式，myfuncPerPartition通过迭代子从分区中拉数据。
 *
 * 这两个方法的另一个区别是，在大数据集情况下的资源初始化开销和批处理：
 * 如果在myfuncPerPartition和myfuncPerElement中都要初始化一个耗时的资源，
 * 然后使用，如数据库链接。在上面的例子中，myfuncPerPartition只需初始化3个资源（
 * 3个分区每个1次），而myfuncPerElement要初始化10次（10个元素每个1次）。
 * 显然，在大数据集情况下（数据集中元素个数远大于分区`数），
 * mapPartitons的开销要小很多，
 * 也便于进行批处理操作。
 */
object MapPartitionsSuit {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.INFO)

        val sparkConf = new SparkConf()
        sparkConf.setMaster("local[*]").setAppName("app")
        val sc = new SparkContext(sparkConf)

        val a = sc.parallelize(1 to 10, 3)
        def myfuncPerElement(e: Int) = {
            println("e=" + e)
            e * 2
        }
        def myfuncPerPartition(iter:Iterator[Int]) = {
            println("run in Partition")
            val res = for(e <- iter) yield  e * 2
            res
        }
        a.map(myfuncPerElement).collect.foreach(println)
        a.mapPartitions(myfuncPerPartition).collect.foreach(println)
    }

}
