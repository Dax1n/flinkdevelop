package com.daxin.socketcep


import java.util

import org.apache.flink.cep.PatternFlatSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import scala.collection.Map

/**
  * Created by Daxin on 2017/6/14.
  */
case class SocketEvent(id: Int, name: String)

case class SocketAlert(id: Int, name: String)

object Main {


  def main(args: Array[String]) {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("node", 9999).map {
      x => val fs = x.split(" ")
        SocketEvent(fs(0).toInt, fs(1))
    }

    val pattern = Pattern.begin[SocketEvent]("start").where(x => x.id > 6).next("middle").where(x => x.id < 8)
    //next和followedBy的主要区别是next必须严格连续的，followedBy是相对顺序满足就可以，不要求严格连续
    //val pattern = Pattern.begin[SocketEvent]("start").where(x => x.id > 20).followedBy("next").where(x => x.id > 20).followedBy("follow").where(x => x.id < 10)


    //设置迭代条件Iterative Conditions
//    val pattern = Pattern.begin[SocketEvent]("daxin").oneOrMore.where(event => {
//      event.id > 6
//    })


    //Simple Conditions
    val simplePattern = Pattern.begin[SocketEvent]("daxin").where(event => event.name.startsWith("maomao"))

    //也可以限制时间的类型
    //start.subtype(classOf[SubEvent]).where(subEvent => ... /* some condition */)


    //组合条件
    //where.(...).where(...)关系都是AND关系，因此我们可以使用or方法创建or的关系
    val combiningPattern = Pattern.begin[SocketEvent]("daxin").where(event => event.name.startsWith("maomao")).or(event => event.id < 5)


    val timePattern = Pattern.begin[SocketEvent]("daxin").times(5).consecutive()

    val timeAllowPattern = Pattern.begin[SocketEvent]("daxin").times(5).allowCombinations()

    //定义最大时间间隔
    val inSomeTimeAllowPattern = Pattern.begin[SocketEvent]("daxin").times(5).within(Time.seconds(10)).allowCombinations()

    val stream = CEP.pattern[SocketEvent](data, timePattern)


    /**
      *
      * @param stringToEvents 参数为Iterable原因是，当时用循环pattern时候会有多个event
      *                       <br>
      *                       The reason for returning an iterable of accepted events for each pattern
      *                       is that when using looping patterns (e.g. oneToMany() and times()), more than
      *                       one event may be accepted for a given pattern.
      * @return
      */
    def createAlert(stringToEvents: Map[String, Iterable[SocketEvent]]): SocketAlert = {


      for (e <- stringToEvents) {
        println("map key = " + e._1)
        for (r <- e._2) {
          println("r = " + r)
        }
      }

      SocketAlert(stringToEvents.size, "--->")
    }

    val result: DataStream[SocketAlert] = stream.select(createAlert(_))




//    //如下是示例代码，并没有实际意义
//    val flatFunc = new PatternFlatSelectFunction[SocketEvent, SocketAlert]() {
//      override def flatSelect(pattern: util.Map[String, util.List[SocketEvent]], out: Collector[SocketAlert]): Unit = {
//
//        //开始个id
//        val startEvent = pattern.get("start").get(1).id
//        //结束id
//        val endEvent = pattern.get("end").get(1).id
//        //发送startEvent+endEvent个时间出去
//        for (i <- 0 to (startEvent+endEvent)) {
//          out.collect(SocketAlert(1, "endEvent"))
//        }
//      }
//    }
//
//    val flatResult = stream.flatSelect(flatFunc)

    result.print()

    env.execute()


  }

}
