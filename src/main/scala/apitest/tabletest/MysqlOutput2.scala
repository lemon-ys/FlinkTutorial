package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object MysqlOutput2 {
  def main(args: Array[String]): Unit = {
    // 1. 创建表环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    // 2. 连接外部系统，读取数据，创建输入表
    tableEnv.executeSql(
      """CREATE TABLE inputTable(
        |id STRING,
        |timesp BIGINT ,
        |temp DOUBLE
        |) WITH(
        |'connector' = 'filesystem',
        |'path'      = 'src/main/resources/sensor.txt',
        |'format'    = 'csv'
        |)""".stripMargin)

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

    //3.3 sqlQuery
    val aggTable2 = tableEnv.sqlQuery("""
                                        |select id, count(id) as cnt, sum(temp) as sum_temp
                                        |from inputTable
                                        |where id = 'sensor_1'
                                        |group by id
                                        |""".stripMargin)

    //4.1 注册输出表-mysql (TableSink)
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

    //4.2 注册输出表-文件 (TableSink)
    tableEnv.executeSql(
      """CREATE TABLE outputTable(
        |id STRING,
        |timesp BIGINT ,
        |temp DOUBLE
        |) WITH(
        |'connector' = 'filesystem',
        |'path'      = 'src/main/resources/output',
        |'format'    = 'csv'
        |)""".stripMargin)

    //5.1 将3.1的查询结果插入到outputTable(输出到文件)
    resultTable.executeInsert("outputTable")
    //5.2 将3.3的查询结果插入到jdbcOutputTable(输出到Mysql)
    //方法 Table.executeInsert(tableName)将 Table发送至已注册的TableSink,
    // 该方法通过名称在 catalog中查找TableSink, 并确认Table schema 和 TableSink schema 一致.
    aggTable2.executeInsert("jdbcOutputTable")

    //5.3 直接将表环境里注册的输入表的Query查询结果插入到已注册的外部输出表(TableSink)
    tableEnv.executeSql(
      """insert into jdbcOutputTable
        |select id, count(id) as cnt, sum(temp) as sum_temp
        |from inputTable
        |where id = 'sensor_1'
        |group by id
        |""".stripMargin)

  }
}
