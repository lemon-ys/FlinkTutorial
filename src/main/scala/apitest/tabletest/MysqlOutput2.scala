package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object MysqlOutput2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 2. 连接外部系统，读取数据，创建输入表
    tableEnv.executeSql(
      """CREATE TABLE inputTable(
        |id STRING,
        |timesp BIGINT ,
        |temp DOUBLE
        |) WITH(
        |'connector' = 'filesystem',
        |'path'      = '/Users/lemon/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt',
        |'format'    = 'csv'
        |)""".stripMargin)

    //    val outputPath = "E:\\FlinkProject\\src\\main\\resources\\output.txt"
    //    tableEnv.executeSql("CREATE TABLE outputTable (" +
    //      "  id STRING," +
    //      "  timesp BIGINT," +
    //      "  temp DOUBLE" +
    //      ") WITH (" +
    //      "  'connector' = 'filesystem'," +
    //      "  'path' = 'outputPath'," +
    //      "  'format' = 'csv'" +
    //      ")")

    //Table API
    //3. 转换操作,转成 Table类
    val sensorTable: Table = tableEnv.from("inputTable")

    //3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'timesp, 'temp)
      .filter('id === "sensor_1")
    //3.2 聚合转换
    val aggTable = sensorTable
      .filter('id === "sensor_1")
      .groupBy('id)
      .select('id, 'id.count as 'count, 'temp.sum as 'sum_temp)

    //sqlQuery
    val aggTable2 = tableEnv.sqlQuery("""
                                        |select id, count(id) as cnt, sum(temp) as sum_temp
                                        |from inputTable
                                        |where id = 'sensor_1'
                                        |group by id
                                        |""".stripMargin)

    //注册输出表-mysql
    tableEnv.executeSql(
      """CREATE TABLE jdbcOutputTable(
        |id varchar(20) not null,
        |cnt bigint not null,
        |sum_temp double not null,
        |PRIMARY KEY (id) NOT ENFORCED
        |) WITH(
        |'connector'  = 'jdbc',
        |'url'        = 'jdbc:mysql://175.24.152.40:33190/ads',
        |'table-name' = 'myOutputTable',
        |'driver'     = 'com.mysql.jdbc.Driver',
        |'username'   = 'yaoshun',
        |'password'   = 'vp^98*s$UpTRsebf'
        |)""".stripMargin)

    //输出到文件
    tableEnv.executeSql(
      """CREATE TABLE outputTable(
        |id STRING,
        |timesp BIGINT ,
        |temp DOUBLE
        |) WITH(
        |'connector' = 'filesystem',
        |'path'      = '/Users/lemon/IdeaProjects/FlinkTutorial/src/main/resources/output',
        |'format'    = 'csv'
        |)""".stripMargin)

    aggTable2.executeInsert("jdbcOutputTable")

    resultTable.executeInsert("outputTable")

  }
}
