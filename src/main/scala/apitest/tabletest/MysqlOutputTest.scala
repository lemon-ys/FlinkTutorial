package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
//import org.apache.flink.table.factories._
import org.apache.flink.types.Row

object MysqlOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "E:\\FlinkProject\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    //3. 转换操作,转成 Table类
    val sensorTable: Table = tableEnv.from("inputTable")
    //3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'timestamp, 'temp)
      .filter('id === "sensor_1")

    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count, 'temp.sum as 'sum_temp)

    //4. 输出到MySQL
    val sinkDDl: String =
      """
        |create table jdbcOutputTable(
        |id varchar(20) not null,
        |cnt bigint not null,
        |sum_temp double not null
        |) with(
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://175.24.152.40:33190/ads',
        | 'connector.table' = 'myOutputTable',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'yaoshun',
        | 'connector.password' = 'vp^98*s$UpTRsebf')
        | """.stripMargin

    // 执行 DDL创建表
    tableEnv.sqlUpdate(sinkDDl)
    //aggTable.insertInto("jdbcOutputTable")

    aggTable.toRetractStream[Row].print("agg")

    env.execute()
  }
}
