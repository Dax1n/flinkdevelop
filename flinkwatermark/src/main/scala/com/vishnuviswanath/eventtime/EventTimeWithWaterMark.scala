package com.vishnuviswanath.eventtime

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import Util._
import org.apache.flink.streaming.api.TimeCharacteristic
import com.vishnuviswanath.processtime.Util.ValueAndTimestamp

/**
 * 英文原文：http://vishnuviswanath.com/flink_eventtime.html
 * 水印本质上就是一个时间戳，当Flink的Operator 接收到WaterMark(假设watermark = 2017-6-19 12:00:00:000)之后，Flink Operator就理解为(假设)在
 * watermark = 2017-6-19 12:00:00:000之前没有消息了,因此WaterMark可以被认为是一种告诉Flink系统目前Event Time进度
 */
object EventTimeWithWaterMark {
  def main(args: Array[String]) {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //说白了：watermark就是延迟窗口计算的一种机制，具体延迟多少就看减去多少了
    val text = env.addSource(new ValueAndTimestampWithDelay).assignTimestampsAndWatermarks(new TimestampExtractorWithWaterMarkDelay)

    val counts = text.map {
      (x: String) => (x.split(",")(0), 1)
    }.keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)
    counts.print
    env.execute("ProcessingTime processing example")

  }
}