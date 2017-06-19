package com.vishnuviswanath.processtime

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.util.concurrent.TimeUnit
import java.util.Calendar
import java.util.Date

/**
 * http://vishnuviswanath.com/flink_eventtime.html
 */
package object Util {
  /**
   * 模拟消息发送例子：Case 1: Messages arrive without delay
   */
  class ValueAndTimestamp extends SourceFunction[String] {
    @volatile var isRun = true
    def cancel(): Unit = {
      isRun = false
    }

    def run(sc: SourceContext[String]): Unit = {
      
      val start = 1497872400  //19:23:00
      val time13 = 13 + start
      val time16 = 16 + start
      @volatile var isRun1 = true
      @volatile var isRun2 = true

      while (isRun) {

        
        if ((System.currentTimeMillis()/1000 == time13 ) && isRun1) {
          isRun1 = false
          sc.collect("a," + time13)
          sc.collect("a," + time13)

        } else if ((System.currentTimeMillis()/1000 == time16 ) && isRun2) {
          sc.collect("a," + time16)
          isRun2 = false

        }

      }
    }
  }
  
  
    class ValueAndTimestampWithDelay extends SourceFunction[String] {
    @volatile var isRun = true
    def cancel(): Unit = {
      isRun = false
    }

    def run(sc: SourceContext[String]): Unit = {
      
      /**
       * start设置一个整点以秒为时间戳的作为发送消息的起点
       */
      val start = 1497872940  //19:23:00
      val time13 = 13 + start
      val time16 = 16 + start
      val time19 = 19 + start
      @volatile var isRun1 = true
      @volatile var isRun2 = true
      @volatile var isRun3 = true

      while (isRun) {
        
        if ((System.currentTimeMillis()/1000 == time13 ) && isRun1) {
          isRun1 = false
          sc.collect("a," + time13)

        } else if ((System.currentTimeMillis()/1000 == time16 ) && isRun2) {
          sc.collect("a," + time16)
          isRun2 = false

        }

         else if ((System.currentTimeMillis()/1000 == time19 ) && isRun3) {
          sc.collect("a," + time19)
          isRun3 = false

        }
      }
    }
  }
}