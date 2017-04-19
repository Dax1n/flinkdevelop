package com.daxin.stream

/**
  * Created by Daxin on 2017/4/18.
  */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//学习书签：
//https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html#example-program

object WindowWordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("node", 9999)


    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)//.setParallelism(3)

//    counts.writeAsText()

    env.execute("Window Stream WordCount")
  }
}
