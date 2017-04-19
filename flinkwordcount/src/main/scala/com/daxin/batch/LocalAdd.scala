package com.daxin.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RuntimeContext, IterationRuntimeContext, RichFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.scala.extensions._ //这个DatStream的增强引入

/**
  * Created by Daxin on 2017/4/16.
  */
object LocalAdd /*extends  RichFunction*/ {

  def main(args: Array[String]) {

    val conf =new Configuration()

    val env = ExecutionEnvironment.createLocalEnvironment(conf)

    //StreamExecutionEnvironment.getExecutionEnvironment //

    val words = env.fromCollection(Array("a b","b c","c d","d e","a a a"))

    val wc= words.flatMap(x=>x.split(" ")).map((_,1)).groupBy(0).sum(1)
/*
    //org.apache.flink.api.scala.extensions._ 增强Api的使用
    //增强Api支持匿名的模式匹配
    words.mapWith{

      case (id,name)=> id
      case _ =>

    }


    val counter=new IntCounter()
*/
    wc.print()

    env.execute()

  }
/*
  override def getRuntimeContext: RuntimeContext = ???

  override def setRuntimeContext(t: RuntimeContext): Unit = ???

  override def getIterationRuntimeContext: IterationRuntimeContext = ???

  override def close(): Unit = ???

  override def open(parameters: Configuration): Unit = ???
  */
}
