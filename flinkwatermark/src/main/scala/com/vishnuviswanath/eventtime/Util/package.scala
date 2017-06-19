package com.vishnuviswanath.eventtime

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.util.concurrent.TimeUnit
import java.util.Calendar
import java.util.Date
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * http://vishnuviswanath.com/flink_eventtime.html
 */
package object Util {

  /**
   * 模拟： EventTime based system
   */
  class ValueAndTimestampWithDelay extends SourceFunction[String] {
    @volatile var isRun = true
    def cancel(): Unit = {
      isRun = false
    }

    def run(sc: SourceContext[String]): Unit = {

      /**
       * start设置一个整点以秒为时间戳的作为发送消息的起点
       */
      val start: Long = 1497876000000L  
      val time13 = 13 * 1000 + start
      val time16 = 16 * 1000 + start
      val time19 = 19 * 1000 + start
      @volatile var isRun1 = true
      @volatile var isRun2 = true
      @volatile var isRun3 = true

      while (isRun) {


        if ((System.currentTimeMillis() == time13) && isRun1) {
          println("if1")
          isRun1 = false
          sc.collect("a," + (time13 ))

        } else if ((System.currentTimeMillis() == time16) && isRun2) {
          sc.collect("a," + (time16 ))
          isRun2 = false
          println("if2")
        } else if ((System.currentTimeMillis() == time19) && isRun3) {
          //13秒的消息迟到19秒到
          sc.collect("a," + (time13 ))
          isRun3 = false
          println("if3")
        }
      }
    }
  }
  //------------------------
  class TimestampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {
    override def extractTimestamp(e: String, prevElementTimestamp: Long) = {
      e.split(",")(1).toLong
    }
    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis)
    }
  }
  
  
    //------------------------
  class TimestampExtractorWithWaterMarkDelay extends AssignerWithPeriodicWatermarks[String] with Serializable {
    override def extractTimestamp(e: String, prevElementTimestamp: Long) = {
      e.split(",")(1).toLong
    }
    override def getCurrentWatermark(): Watermark = {
      //延迟5秒计算，这个是以系统的时间戳延迟5秒计算的
      new Watermark(System.currentTimeMillis-5000)
    }
  }

}