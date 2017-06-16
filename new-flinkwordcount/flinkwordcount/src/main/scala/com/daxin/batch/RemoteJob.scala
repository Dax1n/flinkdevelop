package com.daxin.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.{ConfigConstants, Configuration}
//important: this import is needed to access the 'createTypeInformation' macro function
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.triggers.Trigger
/**
  * Created by Daxin on 2017/4/17.
  * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/types_serialization.html#type-information-in-the-scala-api
  */
object RemoteJob {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.createRemoteEnvironment("node", 6123,"C://logs//flink-lib//flinkwordcount.jar")

    val words = env.readTextFile("hdfs://node:9000/word/spark-env.sh")

    val data = words.flatMap(x => x.split("--")).map(x => (x, 1)).groupBy(0).sum(1)

//    println(data.count()) //简单触发作业打印一下个数
    data.print()

  }
}

