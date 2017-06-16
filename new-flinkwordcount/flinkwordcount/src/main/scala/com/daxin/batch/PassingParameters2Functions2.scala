package com.daxin.batch

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by Daxin on 2017/4/18.
  * 我的邮箱: leodaxin@163.com
  */
object PassingParameters2Functions2 {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val toFilter = env.fromElements(1, 2, 3)

    val c = new Configuration()
    c.setInteger("limit", 2)

    val result = toFilter.filter(new RichFilterFunction[Int]() {
      var limit = 0

      override def open(config: Configuration): Unit = {
        limit = config.getInteger("limit", 0) //没有的话返回默认值0
      }

      def filter(in: Int): Boolean = {
        in > limit
      }
    }).withParameters(c)

    result.print()

  }

}
