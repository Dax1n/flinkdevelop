package com.daxin.socketcep

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

import scala.collection.Map


/**
  * Created by Daxin on 2017/6/15.
  */

case class SocketEventWithTimeStamp(id: Int, time: Long)

case class TimeoutEvent(name: String, str: String)

case class ResultEvent(name: String, str: String)

object MainCepTimeOut {




  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //默认时间机制是：TimeCharacteristic.ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
/* 在生成数据源时候生成时间戳和水印
    env.addSource(new SourceFunction[SocketEventWithTimeStamp]() {

      @volatile var isRunning = true

      override def run(ctx: SourceContext[SocketEventWithTimeStamp]): Unit = {
        while (isRunning) {
          val next = SocketEventWithTimeStamp(10, System.currentTimeMillis())
          ctx.collectWithTimestamp(next, System.currentTimeMillis())

          if (next.time % 10000 == 0) {
            ctx.emitWatermark(new Watermark(System.currentTimeMillis())
          }
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

*/
    val data: DataStream[SocketEventWithTimeStamp] = env.socketTextStream("node", 9999).map {
      x => val fs = x.split(" ")
        SocketEventWithTimeStamp(fs(0).toInt, fs(1).toLong)
    } //.keyBy(_.time) //.window(TumblingEventTimeWindows.of(Time.seconds(3)))


    val assignerWithPeriodicWatermarks = new AssignerWithPeriodicWatermarks[SocketEventWithTimeStamp]() {

      var maxTimeStamp = 0L
      var maxOutOrder = 1000L

      override def getCurrentWatermark: Watermark = {
        println("watermark = " + (maxTimeStamp - maxOutOrder))
        new Watermark(maxTimeStamp - maxOutOrder)
      }

      override def extractTimestamp(element: SocketEventWithTimeStamp, previousElementTimestamp: Long): Long = {

        maxTimeStamp = Math.max(element.time, maxTimeStamp)
        println("elementTimestamp = " + element.time + " and previousElementTimestamp = " + previousElementTimestamp)
        element.time
      }
    }



//   data.assignAscendingTimestamps()

    data.assignTimestampsAndWatermarks(assignerWithPeriodicWatermarks)


    val pattern = Pattern.begin[SocketEventWithTimeStamp]("start").where(x => x.id > Integer.MIN_VALUE) //.within(Time.seconds(10))


    val stream = CEP.pattern(data, pattern)

    def create(stringToStamps: Map[String, Iterable[SocketEventWithTimeStamp]]): String = {
      "create ... "
    }

    val result=stream.select(create(_))

/*
    val result: DataStream[Either[TimeoutEvent, ResultEvent]] = stream.select(new PatternTimeoutFunction[SocketEventWithTimeStamp, TimeoutEvent]() {
      override def timeout(pattern: util.Map[String, util.List[SocketEventWithTimeStamp]], timeoutTimestamp: Long): TimeoutEvent =
        TimeoutEvent("TimeoutEvent", pattern.values().toString)
    }, new PatternSelectFunction[SocketEventWithTimeStamp, ResultEvent]() {
      override def select(pattern: util.Map[String, util.List[SocketEventWithTimeStamp]]): ResultEvent = ResultEvent("ResultEvent", pattern.values().toString)
    })
*/

    result.print()


    env.execute()

  }

}
