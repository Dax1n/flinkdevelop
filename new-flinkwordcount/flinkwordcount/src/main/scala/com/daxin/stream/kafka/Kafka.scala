package com.daxin.stream.kafka

import java.util.Properties

import com.daxin.stream.MySQLSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

/**
  * Created by Daxin on 2017/4/27.
  */
object Kafka {
  def main(args: Array[String]) {


    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "node:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "node:2181");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("group.id", "wcount");

    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createRemoteEnvironment("node",6123,"C://logs//flink-lib//flinkwordcount.jar")


    val stream = env.addSource(new FlinkKafkaConsumer010[String]("wcount",new SimpleStringSchema(),properties))

    env.enableCheckpointing(5000)

    stream.print()
    stream.map{
      x=>(x,1)
    }.addSink(new MySQLSink())

    env.execute()

  }

}
