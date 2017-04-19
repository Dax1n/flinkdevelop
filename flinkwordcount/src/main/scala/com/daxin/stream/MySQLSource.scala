package com.daxin.stream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, ParallelSourceFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark

//import org.apache.flink.streaming.connectors.kafka.
/**
  * Created by Daxin on 2017/4/19.
  */
//class MySQLSource extends RichParallelSourceFunction[String] {
//
//
//
//}

object MySQLSource {
  def main(args: Array[String]) {


    val env = StreamExecutionEnvironment.createRemoteEnvironment("node", 6123, "C://logs//flink-lib//flinkwordcount.jar")
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


  }

}
