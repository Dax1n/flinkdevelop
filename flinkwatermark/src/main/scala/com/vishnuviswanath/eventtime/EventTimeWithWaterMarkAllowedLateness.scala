package com.vishnuviswanath.eventtime
  
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import Util._
import org.apache.flink.streaming.api.TimeCharacteristic
import com.vishnuviswanath.processtime.Util.ValueAndTimestamp

object EventTimeWithWaterMarkAllowedLateness {
  def main(args: Array[String]) {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //说白了：watermark就是延迟窗口计算的一种机制，具体延迟多少就看减去多少了
    val text = env.addSource(new ValueAndTimestampWithDelay).assignTimestampsAndWatermarks(new TimestampExtractor)

    val counts = text.map {
      (x: String) => (x.split(",")(0), 1)
    }.keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5)).allowedLateness(Time.seconds(5))
      .sum(1)
    counts.print
    env.execute("ProcessingTime processing example")

  }
}