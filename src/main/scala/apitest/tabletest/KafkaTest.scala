package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object KafkaTest {
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
        |'connector' = 'kafka',
        |'topic'     = 'sensor',
        |'properties.zookeeper.connect' = 'localhost:2181',
        |'properties.bootstrap.servers' = 'localhost:9092',
        |'format'    = 'csv'
        |)""".stripMargin)

    /*//4.1 注册输出表-mysql (TableSink)
    tableEnv.executeSql(
      """CREATE TABLE jdbcOutputTable(
        |id varchar(20) not null,
        |timesp bigint not null,
        |temp double not null,
        |PRIMARY KEY (id) NOT ENFORCED
        |) WITH(
        |'connector'  = 'jdbc',
        |'url'        = 'jdbc:mysql://175.24.152.40:33190/ads',
        |'table-name' = 'sensor_table',
        |'driver'     = 'com.mysql.jdbc.Driver',
        |'username'   = 'yaoshun',
        |'password'   = 'vp^98*s$UpTRsebf'
        |)""".stripMargin)

    //5.3 直接将表环境里注册的输入表的Query查询结果插入到已注册的外部输出表(TableSink)
    tableEnv.executeSql(
      """insert into jdbcOutputTable
        |select id, timesp, temp
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin)*/

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

    tableEnv.executeSql(
      """insert into jdbcOutputTable
        |select id, count(id) as cnt, sum(temp) as sum_temp
        |from inputTable
        |where id = 'sensor_1'
        |group by id
        |""".stripMargin)


  }
}
