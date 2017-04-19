package com.daxin.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration

/**
  * 结果输出：
  *
  *
  */
object WriteOut {

  def main(args: Array[String]) {

    //
    val conf = new Configuration()

    val env = ExecutionEnvironment.createLocalEnvironment(conf)

     env.getConfig

    //StreamExecutionEnvironment.getExecutionEnvironment //
//
//    val words:DataSet[String] = env.fromCollection(Array("a b", "b c", "c d", "d e", "a a a"))
//
//    val wc = words.flatMap(x => x.split(" ")).map((_, 1)).groupBy(0).sum(1)
//
//
//    //writeAsText底层调用的是：org.apache.flink.api.java.ExecutionEnvironment.registerDataSink
//    //registerDataSink注释：Adds the given sink to this environment. Only sinks that have been
//    // added will be executed once the execute() or execute(String) method is called.
//    //TODO 言外之意，writeAsText方法只有当execute方法被调用才会真正写
//    wc.writeAsText("/home/daxin")
//
//
//
//    wc.print()

    env.execute()

  }

}
