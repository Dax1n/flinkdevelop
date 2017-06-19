package com.vishnuviswanath.processtime

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import Util._


object ProcessTime {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    val text = env.socketTextStream("localhost", 9999)
//    val text = env.addSource(new ValueAndTimestamp) //(a,2), (a,3) and (a,1) 
    val text = env.addSource(new ValueAndTimestampWithDelay) //(a,1), (a,3) and (a,2) 

    val counts = text.map {
      (x: String) => (x.split(",")(0), 1)
    }.keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)

    counts.print
    env.execute("ProcessingTime processing example")

  }

}

