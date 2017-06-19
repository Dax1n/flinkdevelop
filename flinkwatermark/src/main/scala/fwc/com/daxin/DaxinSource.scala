package fwc.com.daxin

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Created by Daxin on 2017/6/19.
 */
object DaxinSource {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val source = new SourceData

    val data = env.addSource(source).name("source-daxin")
    //
    val result = data.map((_, 1)).name("map-daxin").keyBy(0).sum(1).name("sum-daxin")
    result.print()

    env.execute("word count !")

  }

}

class SourceData extends SourceFunction[String] {
  @volatile var isRun = true

  override def cancel(): Unit = {
    isRun = false
  }

  override def run(ctx: SourceContext[String]): Unit = {
    while (isRun) {
//      println(ctx.getClass)
      ctx.collect(UUID.randomUUID().toString)
      TimeUnit.SECONDS.sleep(1)
    }
  }
}
