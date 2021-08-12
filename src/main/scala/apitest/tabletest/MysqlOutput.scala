package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object MysqlOutput {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 2. 连接外部系统，读取数据，注册表
    val filePath = "E:\\FlinkProject\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("timesp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

/*    tableEnv.executeSql("CREATE TABLE inputTable (" +
      "  id STRING," +
      "  timesp BIGINT," +
      "  temp DOUBLE" +
      ") WITH (" +
      "  'connector' = 'filesystem'," +
      "  'path' = 'E:\\FlinkProject\\src\\main\\resources\\sensor.txt'," +
      "  'format' = 'csv'" +
      ")")*/

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

    //3. 转换操作,转成 Table类
    val sensorTable: Table = tableEnv.from("inputTable")
    //3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'timesp, 'temp)
      .filter('id === "sensor_1")

    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count, 'temp.sum as 'sum_temp)

    tableEnv.executeSql("CREATE TABLE jdbcOutputTable (\n" +
      "    id varchar(20) not null,\n" +
      "    cnt bigint not null,\n" +
      "    sum_temp double not null,\n" +
      "    PRIMARY KEY (id) NOT ENFORCED" +
      ") WITH (\n" +
      "   'connector'  = 'jdbc',\n" +
      "   'url'        = 'jdbc:mysql://175.24.152.40:33190/ads',\n" +
      "   'table-name' = 'myOutputTable',\n" +
      "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
      "   'username'   = 'yaoshun',\n" +
      "   'password'   = 'vp^98*s$UpTRsebf'\n" +
      ")")

    aggTable.executeInsert("jdbcOutputTable")

  }
}
