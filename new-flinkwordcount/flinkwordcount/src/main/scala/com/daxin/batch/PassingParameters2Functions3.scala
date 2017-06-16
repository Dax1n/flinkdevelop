package com.daxin.batch

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RichFilterFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by Daxin on 2017/4/18.
  * 传递参数3：Globally via the ExecutionConfig
  *
  */
object PassingParameters2Functions3 {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("1", "2")

    val conf = new Configuration()
    conf.setString("mykey", "2")
    env.getConfig.setGlobalJobParameters(conf)

    class RichFunc extends RichMapFunction[String, String] {
      var mykey: String = _

      override def open(parameters: Configuration): Unit = {

        super.open(parameters)

        val globalParams: ExecutionConfig.GlobalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
        val globConf = globalParams.asInstanceOf[Configuration]
        mykey = globConf.getString("mykey", "default")

      }

      override def map(value: String): String = {
        if (mykey.equals(value)) "is equals" else "not equals"
      }
    }

    data.map(new RichFunc).print()

  }

}
