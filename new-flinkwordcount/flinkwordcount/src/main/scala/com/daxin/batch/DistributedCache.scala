package com.daxin.batch

import java.io.{FileReader, BufferedReader}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.HashMap

/**
  *
  * Created by Daxin on 2017/4/18.
  *
  */
object DistributedCache {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    //本地IDE运行，缓存的是本地文件，缓存文件名字为cahce
    env.registerCachedFile("file:///C://logs//flink", "cache")

    val data = env.fromElements("111", "222", "333")

    val result = data.map(new RichMapFunction[String, String] {


      val map = HashMap[String, String]()

      override def open(parameters: Configuration): Unit = {

        val file = getRuntimeContext.getDistributedCache.getFile("cache")//获取缓存文件
        //读取缓存文件内容到HashMap中，这个也可以使用广播实现
        val br = new BufferedReader(new FileReader(file))
        var line = br.readLine()

        while (line != null) {
          map.put(line, line + "" + line)
          line = br.readLine()
        }

      }

      override def map(value: String): String = {

        map.getOrElse(value, "default") //返回该value在map中的value值，如果不存在key为value的返回默认default

      }
    })

    result.print() //执行作业

  }

}
