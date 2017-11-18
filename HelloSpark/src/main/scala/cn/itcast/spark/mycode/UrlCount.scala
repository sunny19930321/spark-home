package cn.itcast.spark.mycode


import java.net.URL
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liyang on 2017/11/18.
  */

object UrlCount {
  val courseMap = Map("java" -> 0, "php" -> 0, "net" -> 0)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("H:\\project\\spark\\HelloSpark\\itcast.log").map(
      line =>{
        val s = line.split("\t")
        (s(1),1)
      }
    ).reduceByKey(_+_).map(
      urlmap => {
        val host = new URL(urlmap._1).getHost
        (host,urlmap._2)
      }
    ).reduceByKey(_+_)

    println(rdd1.collect().toBuffer)
    sc.stop()
  }
}
