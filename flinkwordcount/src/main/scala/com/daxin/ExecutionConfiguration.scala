package com.daxin

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.scala.extensions._

import  org.apache.flink.types._
/**
  * Created by Daxin on 2017/4/17.
  * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/execution_configuration.html
  *
  */
object ExecutionConfiguration {
  def main(args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val conf = env.getConfig //获取执行环境的配置
    //conf进行如下配置
    //    conf.setExecutionRetryDelay(5)
    //    conf.disableClosureCleaner()
    conf.setParallelism(2) //设置并行度

//    val arr=Array[String]("a b", "b c", "c d", "d e", "a a a")
//    val words = env.fromCollection(arr)
//
//    val wc = words.flatMap(x => x.split(" ")).map((_, 1)).groupBy(0).sum(1)
//
//    wc.print()

    env.execute()
  }
}
