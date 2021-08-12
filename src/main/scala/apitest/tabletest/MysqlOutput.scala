package apitest.tabletest

import org.apache.flink.api.scala._
import org.apache.flink.table.api._

object MysqlOutput {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val settings = EnvironmentSettings.newInstance.build
    val tableEnv = TableEnvironment.create(settings)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "E:\\FlinkProject\\src\\main\\resources\\sensor.txt"

    tableEnv.

  }

}
