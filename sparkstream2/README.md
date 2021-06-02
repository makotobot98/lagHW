requirement: 

- consuming Kafka topic inside a Spark structured streaming application
- write altitude information into Mysql database

 

- `MysqlWriter.scala`
  - 创建jdbc连接池，并重写`ForeachWriter[BusInfo]`

```scala
package com.makoto.sparkStream
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.ForeachWriter
import java.sql.Statement

object MysqlWriter {
  val url = "jdbc:mysql://localhost/mysql"
  val username = "root"
  val password = "123456788"
  val tableName = "lagi"

  val driver: DriverManager = null
  def getConn(): Connection = {
    return DriverManager.getConnection(url, username, password)
  }
}

class MysqlWriter extends ForeachWriter[BusInfo] {
  var sqlConn: Connection = null
  override def open(partitionId: Long, epochId: Long): Boolean = {
    sqlConn = MysqlWriter.getConn()
    true
  }

  override def process(value: BusInfo): Unit = {
    var stmt = sqlConn.createStatement()
    val sql = s"INSERT INTO ${MysqlWriter.tableName} VALUES (${value.deployNum}, ${value.plateNum}, ${value.lglat})"
    stmt.executeUpdate(sql)
  }

  override def close(errorOrNull: Throwable): Unit = {
    sqlConn.close()
  }
}

```



- Main

```scala
object KafkaScalaMysqlSync {
  def main(args: Array[String]): Unit = {
    //1 获取sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("kafka-redis")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    //2 定义读取kafka数据源
    val kafkaDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop4:9092")
      .option("subscribe", "lagou_bus_info")
      .load()
    //3 处理数据
    val kafkaValDf: DataFrame = kafkaDf.selectExpr("CAST(value AS STRING)")
    //转为ds
    val kafkaDs: Dataset[String] = kafkaValDf.as[String]
    val busInfoDs: Dataset[BusInfo] = kafkaDs.map(msg => {
      BusInfo(msg)
    })
    val writer = new MysqlWriter
    //4 输出,写出数据到redis和hbase
    busInfoDs.writeStream
      .foreach(
        writer
      )
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}

```

- `BusInfo` case class

```scala
package com.makoto.sparkStream

case class BusInfo(
                    deployNum: String,
                    simNum: String,
                    transpotNum: String,
                    plateNum: String,
                    lglat: String,
                    speed: String,
                    direction: String,
                    mileage: String,
                    timeStr: String,
                    oilRemain: String,
                    weight: String,
                    acc: String,
                    locate: String,
                    oilWay: String,
                    electric: String
                  )
object BusInfo {
  def apply(
             msg: String
           ): BusInfo = {
    val arr: Array[String] = msg.split(",")
    new BusInfo(
      arr(0),
      arr(1),
      arr(2),
      arr(3),
      arr(4),
      arr(5),
      arr(6),
      arr(7),
      arr(8),
      arr(9),
      arr(10),
      arr(11),
      arr(12),
      arr(13),
      arr(14)
    )
  }
}

```

