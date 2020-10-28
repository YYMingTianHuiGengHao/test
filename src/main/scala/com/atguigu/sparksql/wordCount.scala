package com.atguigu.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yymstart
 * @create 2020-10-27 22:58
 */
object wordCount {
  //数据准备及环境连接：
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")
  val sc = new SparkContext(sparkConf)

  val rdd1: RDD[String] = sc.makeRDD(List("hadoop scala", "spark spark hello", "scala"), 2)

  //方法1：groupBy + map
  rdd1.flatMap(_.split(" "))
    .groupBy(word => word)
    .map {
      case (word, iter) => {
        (word, iter.size)
      }
    }
    .collect()
    .foreach(println)
  println("=======如上方法1========")

  //方法2:reduceByKey
  rdd1.flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)
    .collect()
    .foreach(println)
  println("========如上方法2========")

  //方法3:aggregateByKey
  rdd1.flatMap(_.split(" "))
    .map((_,1))
    .aggregateByKey(0)(_+_,_+_)
    .collect()
    .foreach(println)
  println("======如果方法3======")
}
