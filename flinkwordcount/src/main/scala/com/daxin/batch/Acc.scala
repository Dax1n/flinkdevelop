package com.daxin.batch

import java.io.File

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by Daxin on 2017/4/18.
  */
object Acc {
  def main(args: Array[String]) {


    val numLines = new IntCounter()
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("1", "2", "5")

    //TODO RichMapFunction函数可以访问RuntimeContext
    val wm = data.map(new RichMapFunction[String, String]() {

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("daxinCounter", numLines)
      }

      override def map(value: String): String = {

        numLines.add(2)
        "111"
      }
    })

    val file = new File("C:\\logs\\flink")
    if (file.exists()) {
      file.delete()
    }
    wm.writeAsText("C:\\logs\\flink") //可以正常执行
    // wm.printToErr()  //异常：No new data sinks have been defined since the last execution.

    //println(env.getExecutionPlan())

    val rs = env.execute()

    val counter: Int = rs.getAccumulatorResult("daxinCounter")

    //println("counter : " + counter)

  }


}
