package apitest.sinktest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Test1 {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val value = env.readTextFile("E:\\FlinkProject\\src\\main\\resources\\sensor.txt")


    value.print("stream1")
    env.execute("source test job")
  }
}
