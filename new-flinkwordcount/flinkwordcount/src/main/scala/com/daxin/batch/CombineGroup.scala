package com.daxin.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.util.Collector
import  org.apache.flink.api.scala._

/**
  * Created by Daxin on 2017/4/18.
  */
object CombineGroup {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input: DataSet[Tuple1[String]] = env.fromElements("hello","spark","flink","hello","spark","flink").map(Tuple1(_))



    val combinedWords: DataSet[(String, Int)] = input
      .groupBy(0)
      .combineGroup {
        (words, out: Collector[(String, Int)]) =>
          var key: String = null
          var count:Int = 0

          for (word <- words) {
            key = word._1
            count += 1
          }
          out.collect((key, count))
      }

    combinedWords.print()



    val output: DataSet[(String, Int)] = combinedWords
      .groupBy(0)
      .reduceGroup {
        (words, out: Collector[(String, Int)]) =>
          var key: String = null
          var sum1 = 0

          for ((word, sum) <- words) {
            key = word
            sum1 += 1
          }
          out.collect((key, sum1))
      }


    output.print()

  }

}
