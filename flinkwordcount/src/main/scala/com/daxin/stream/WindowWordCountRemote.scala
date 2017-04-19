package com.daxin.stream

/**
  * Created by Daxin on 2017/4/18.
  */

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//import org.apache.flink.contrib.streaming.DataStreamUtils

//学习书签：
//https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html#example-program

object WindowWordCountRemote {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.createRemoteEnvironment("node", 6123, "C://logs//flink-lib//flinkwordcount.jar")
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2) //最少的有两个线程才会输出
     val text = env.socketTextStream("node", 9996)


    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)//.setParallelism(3)
    //TODO 在集群模式下打印WebUI的TaskManger的Stdout中
   // counts.print()



//     val sink = counts.addSink(new PrintSinkFunction[Tuple2[String,Int]]())


     val sink = counts.addSink(new MySQLSink())

     //sink.setParallelism(1).name("sink-daxin")

   //  counts.writeAsText("/word/countflink/")
    env.execute("Window Stream WordCount")
  }
}
