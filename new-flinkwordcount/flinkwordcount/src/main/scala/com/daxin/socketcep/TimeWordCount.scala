package com.daxin.socketcep

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
/**
  * Created by Daxin on 2017/6/16.
  */
case class TimeWord(time: Long, word: String,one:Int)

object TimeWordCount {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd=HH:mm:ss:SSS")

    val hostName = "node"
    val port = 9999

    /**
      * Step 1. Obtain an execution environment for DataStream operation
      * set EventTime instead of Processing Time
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
      * Step 2. Create DataStream from socket
      */
    val input = env.socketTextStream(hostName, port)

    /**
      * Step 3. Implement '分钟成交量' logic
      */

    /**
      * parse input stream to a new Class which is implement the Map function
      */
    val parsedStream = input.map{
      x=>

        TimeWord(format.parse(x.split(" ")(0)).getTime,x.split(" ")(1),1)
    }


    /**
      * assign Timestamp and WaterMark for Event time: eventTime(params should be a Long type)
      */
    val timeValue = parsedStream.assignAscendingTimestamps(_.time)

    val sumVolumePerMinute = timeValue
      .keyBy(_.word)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .sum(2)//index start from : 0
      .name("sum volume per minute")


    /**
      * Step 4. Sink the final result to standard output(.out file)
      */


    sumVolumePerMinute.map{
      x=>
        format.format(new Date(x.time))+" "+x.word+" "+x.one
    }.print()


    /**
      * Step 5. program execution
      */

    env.execute("SocketTextStream for sum of volume Example")


  }


}
