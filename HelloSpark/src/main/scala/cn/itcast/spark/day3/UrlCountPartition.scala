package cn.itcast.spark.day3

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by root on 2016/5/18.
  */
object UrlCountPartition {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("H:\\project\\spark\\HelloSpark\\src\\main\\files\\usercount\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })

    val ints = rdd3.map(_._1).distinct().collect()

    val hostParitioner = new HostParitioner(ints)

    //val rdd4 = rdd3.partitionBy(new HashPartitioner(ints.length))

    /**
      *     //    val rdd33 = rdd3.collect().toBuffer
           //    println(rdd33)
      * ArrayBuffer((php.itcast.cn,(http://php.itcast.cn/php/course.shtml,459)),
      * (java.itcast.cn,(http://java.itcast.cn/java/video.shtml,496)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/base.shtml,543)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/android.shtml,501)),
      * (net.itcast.cn,(http://net.itcast.cn/net/video.shtml,521)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/hadoop.shtml,506)),
      * (net.itcast.cn,(http://net.itcast.cn/net/course.shtml,521)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/cloud.shtml,1028)),
      * (php.itcast.cn,(http://php.itcast.cn/php/video.shtml,490)),
      * (java.itcast.cn,(http://java.itcast.cn/java/teacher.shtml,482)),
      * (php.itcast.cn,(http://php.itcast.cn/php/teacher.shtml,464)),
      * (net.itcast.cn,(http://net.itcast.cn/net/teacher.shtml,512)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/javaee.shtml,1000)),
      * (java.itcast.cn,(http://java.itcast.cn/java/course/javaeeadvanced.shtml,477)))
      */

    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    rdd4.saveAsTextFile("H:\\project\\spark\\HelloSpark\\src\\main\\out")


    //println(rdd4.collect().toBuffer)
    sc.stop()

  }
}

/**
  * 决定了数据到哪个分区里面
  * @param ins
  */
class HostParitioner(ins: Array[String]) extends Partitioner {

  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for(i <- ins){
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString, 0)
  }
}