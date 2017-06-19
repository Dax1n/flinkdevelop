package com.vishnuviswanath.eventtime

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import Util._
import org.apache.flink.streaming.api.TimeCharacteristic

object EventTime {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //正常结果是：(a,2), (a,3) and (a,1) 
    //实际结果是：(a,1), (a,3) and (a,1)，证明2和3窗口是正确的，由于迟到的消息在处理系统的19秒到来抽取事件时间是13秒，所以在15-25的时间窗口不计算 
    /**
     * The results look better, the windows 2 and 3 now emitted correct result,
     *  but window1 is still wrong. Flink did not assign the delayed message to
     * window 3 because it now checked the message’s event time and understood
     * that it did not fall in that window. But why didn’t it assign the message
     * to window 1?. The reason is that by the time the delayed message reached
     * the system(at 19th second), the evaluation of window 1 has already
     * finished (at 15th second). Let us now try to fix this issue by using the Watermark.
     * <br>大概意思就是迟到的元素在19秒到来时候，窗口1已经计算完毕了，所以如果想得到计算结果就需要使用WaterMark
     */
    val text = env.addSource(new ValueAndTimestampWithDelay).assignTimestampsAndWatermarks(new TimestampExtractor)

    val counts = text.map {
      (x: String) => (x.split(",")(0), 1)
    }.keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)

    counts.print
    env.execute("ProcessingTime processing example")

  }
}