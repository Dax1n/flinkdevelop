package com.daxin


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.{ConfigConstants, Configuration}

import scala.reflect.ClassTag

//important: this import is needed to access the 'createTypeInformation' macro function
import org.apache.flink.api.scala._

/**
  * Created by Daxin on 2017/4/17.
  * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/types_serialization.html#type-information-in-the-scala-api
  *
  */
object RemoteJob {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.createRemoteEnvironment("node", 6123)


    val words: DataSet[String] = env.readTextFile("hdfs://node:9000/word/hadoop1.txt")
//    def func(x:String):(String,Int)=(x,1)
//    val m = words.map(x => func(x))
    //TODO 写了好久才解决问题
    implicit val  typeInformation=TypeInformation.of(classOf[String])
    implicit val ct = ClassTag[String](classOf[String])

    words.flatMap(x=>x.split(" "))
    println("wc.count(): " +words.count())
//    env.execute()
  }
}


//
//    implicit  val ti1 =TypeInformation.of(classOf[Array[String]])  //:DataSet[Array[String]]
//    implicit  val ti2 =TypeInformation.of(classOf[String])  //:DataSet[Array[String]]
//    implicit  val ti3 =TypeInformation.of(classOf[Tuple2[String,Int]])  //:DataSet[Array[String]]
//
//    val flatMap:DataSet[Tuple2[String,Int]] = words.flatMap(x=>x.split(" ")).map(x=> (x,1)).groupBy(0).sum(1)

//    implicit object obj[Tuple2[String,Int]] extends TypeInformation[Tuple2[String,Int]]

//    val func=(x:String)=>(x,1)