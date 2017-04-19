package com.daxin.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

//asScala需要使用隐式转换

/**
  * Created by Daxin on 2017/4/16.
  */
object Broadcast {

  def main(args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBroadcast = env.fromElements(1, 2, 3)

    val data = env.fromElements("1", "2", "5")

    /**
      * 如下是RichMapFunction的注释：
      * Rich variant of the MapFunction. As a RichFunction, it gives access to
      * the RuntimeContext and provides setup and teardown methods:
      * RichFunction.open(org.apache.flink.configuration.Configuration) and RichFunction.close().
      * <br>RichMapFunction是MapFunction的变体，RichFunction可以访问运行时上下文（RuntimeContext）
      * 并提供开启和关闭方法
      * <br>
      */
    val result = data.map(new RichMapFunction[String, String]() {
      var broadcastSet: Traversable[Integer] = null

      override def open(config: Configuration): Unit = {
        // 3. Access the broadcasted DataSet as a Collection
        broadcastSet = getRuntimeContext().getBroadcastVariable[Integer]("broadcastSetName").asScala
      }

      def map(in: String): String = {
        //...
        if (broadcastSet.toList.contains(in.toInt))
          in //随便简单返回字符串
        else
          in + "  " + broadcastSet.toList.size + "   " + broadcastSet.toList.contains(in) + "   " + broadcastSet.toList(0).getClass //随便简单返回
      }
    }).withBroadcastSet(toBroadcast, "broadcastSetName") // 2. Broadcast the DataSet



    result.print()
  }

}


// data.withParameters()