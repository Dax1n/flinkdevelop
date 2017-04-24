package com.daxin.stream.cep


import java.util
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._


/**
  * Created by Daxin on 2017/4/24.
  *
  * 坑爹错误：由于之前写的程序是Java版本，之后改写Scala版本时候就把import 包直接复制到Scala文件的包上，然后报错,原因是：
  * 调用了Scala的方法，返回值使用了Java的类型接受，所以报错
  *
  *
  *
  */
object CepApp {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(new TemperatureEvent("xyz", 22.0),
      new TemperatureEvent("xyz", 20.1), new TemperatureEvent("xyz", 21.1), new TemperatureEvent("xyz", 22.2),
      new TemperatureEvent("xyz", 29.1), new TemperatureEvent("xyz", 22.3), new TemperatureEvent("xyz", 22.1),
      new TemperatureEvent("xyz", 22.4), new TemperatureEvent("xyz", 22.7),
      new TemperatureEvent("xyz", 27.0))

    val pattern:Pattern[TemperatureEvent,_] = Pattern.begin[TemperatureEvent]("first").subtype(classOf[TemperatureEvent]).where(new FilterFunction[TemperatureEvent] {
      override def filter(value: TemperatureEvent): Boolean = {
        if (value.temperature >= 26.0) true else false

      }
    }).within(Time.seconds(10))


    val patternStream: DataStream[Alert] = CEP.pattern[TemperatureEvent](input, pattern).select(new PatternSelectFunction[TemperatureEvent, Alert] {
      override def select(pattern: util.Map[String, TemperatureEvent]): Alert = {

        new Alert("Temperature Rise Detected ...")
      }
    })

    patternStream.print()
    env.execute("CEP on Temperature Sensor")



  }

}
