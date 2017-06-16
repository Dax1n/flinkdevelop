package com.daxin.stream.com.daxin.watermark

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//TODO watermark=事件最大时间戳-允许的最大乱序时间
//TODO 当水印大于事件的时间戳而且达到窗口结束时间时候出发窗口计算！

object WatermarkTest {

  def main(args: Array[String]): Unit = {

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val hostName = "node"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.setAutoWatermarkInterval(10 * 1000)



    env.setParallelism(1)
    println("env.getParallelism = " + env.getParallelism)
    val input = env.socketTextStream(hostName, port)

    val inputMap = input.map(f => {
      val arr = f.split("\\W+")
      val code = arr(0)
      val time = arr(1).toLong
      (code, time)
    })

    val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      /**
        * 指的是接收到所有事件中最大的时间戳
        */
      var currentMaxTimestamp = 0L
      /**
        * 最大允许的乱序时间是10s
        */
      val maxOutOfOrderness = 10000L

      var a: Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {

        println("Time : " + format.format(new Date()) + " currentMaxTimestamp = " + currentMaxTimestamp+"(" + format.format(new Date(currentMaxTimestamp))+") maxOutOfOrderness = " + maxOutOfOrderness + "  getCurrentWatermark=>  = " + (currentMaxTimestamp - maxOutOfOrderness)+"(" + format.format(new Date(currentMaxTimestamp - maxOutOfOrderness))+")")
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }

      //TODO 这个方法作用是获取当前该事件中的eventTime时间戳的
      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timestamp = t._2
        //修改当前最大事件时间戳
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("extractTimestamp => the max value between " + timestamp + " and " + currentMaxTimestamp + " is : " + currentMaxTimestamp)
        timestamp
      }
    })


    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))//TODO 设置允许迟到20秒的数据到来.allowedLateness(Time.seconds(20))
      .apply(new WindowFunctionTest)

    window.print()

    env.execute()
  }


}

class WindowFunctionTest extends WindowFunction[(String, Long), (String, Int, String, String, String, String), String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    out.collect((key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd)))
  }

}