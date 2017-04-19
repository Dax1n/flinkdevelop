package com.daxin.batch

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * Created by Daxin on 2017/4/18.
  */
object PassingParameters2Functions1 {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val toFilter = env.fromElements(1, 2, 3)
    class MyFilter(limit: Int) extends FilterFunction[Int] {
      override def filter(value: Int): Boolean = {
        value > limit
      }
    }
    val result =toFilter.filter(new MyFilter(2))

    result.print()

  }

}
