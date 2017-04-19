package com.daxin.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala.extensions._

//增强的引入
/**
  * Created by Daxin on 2017/4/17.3
  *
  * Scala API Extensions
  *
  * url:https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/scala_api_extensions.html
  *
  * 博客：http://blog.csdn.net/dax1n/article/details/70209606
  *
  *如下是官方demo：
  * object Main {
  * import org.apache.flink.api.scala.extensions._
  * case class Point(x: Double, y: Double)
  * def main(args: Array[String]): Unit = {
  * val env = ExecutionEnvironment.getExecutionEnvironment
  * val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
  * ds.filterWith {
  * case Point(x, _) => x > 1
  * }.reduceWith {
  * case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
  * }.mapWith {
  * case Point(x, y) => (x, y)
  * }.flatMapWith {
  * case (x, y) => Seq("x" -> x, "y" -> y)
  * }.groupingBy {
  * case (id, value) => id
  * }
  * }
  * }
  *
  */
object Extension {

  def main(args: Array[String]) {

    val conf = new Configuration()

    val env = ExecutionEnvironment.createLocalEnvironment(conf)

    //StreamExecutionEnvironment.getExecutionEnvironment //
//
//    val words = env.fromCollection[String](Array("a b", "b c", "c d", "d e", "a a a"))
//
//    val wc = words.flatMap(x => x.split(" ")).map((_, 1)).groupBy(0).sum(1)
//
////    //org.apache.flink.api.scala.extensions._ 增强Api的使用
////    //增强Api支持匿名的模式匹配
////    words.mapWith {
////
////      case (id, name) => id
////      case _ =>
////
////    }
////
////    val counter = new IntCounter()
//
//    wc.print()

    env.execute()

  }

}
