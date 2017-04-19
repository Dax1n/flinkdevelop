package com.daxin.batch

import org.apache.flink.api.scala._
/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val lines = env.readTextFile("hdfs://node:9000/word/hadoop1.txt")
    val wc= lines.flatMap(x=>x.split(" ")).map((_,1)).groupBy(0).sum(1)



    wc.writeAsText("hdfs://node:9000/flink/wordcount/")

    env.execute("flinkWordCount")

  }

}
