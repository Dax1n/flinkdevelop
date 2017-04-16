package com.daxin

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.sinks.CsvTableSink

/**
  * Created by Daxin on 2017/4/16.
  */
object Local {

  def main(args: Array[String]) {

    val conf =new Configuration()

    val env = ExecutionEnvironment.createLocalEnvironment(conf)

    val words = env.fromCollection(Array("a b","b c","c d","d e","a a a"))

    val wc= words.flatMap(x=>x.split(" ")).map((_,1)).groupBy(0).sum(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)


    tableEnv.registerDataSet("wc",wc)


    val sink=new CsvTableSink("C:\\迅雷下载\\flinkwordcount\\sink","-")
    tableEnv.sql("select * from wc").writeToSink(sink)
    println("---------------------------------------------------")

    //groupBy算子直接返回一个： new GroupedDataSet，这个算子应该是一个transform
    // val group = words.flatMap(x=>x.split(" ")).map((_,1)).groupBy(0)

    wc.print()
  }

}
