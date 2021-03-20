package com.makoto.sparkStream
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaConsumer {
  val GROUP_ID = "testId"
  val SUB_TOPICS: Array[String] = Array("topicA")
  val PUSH_TOPIC = "topicB"
  val BROKERS = "linux121:9092,linux122:9092,linux123:9092"


  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(2))


    // 定义kafka相关参数
    val kafkaParams: Map[String, Object] = getKafkaConsumerParameters(GROUP_ID)

    // 从 kafka 中获取数据
    val dstream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](SUB_TOPICS,
          kafkaParams)
      )

    //定义输出kafka producer
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])
    val producer = new KafkaProducer[String, String](prop)

    // DStream输出到topicB
    dstream.foreachRDD{(rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s">>> Processing *********** rdd.count = ${rdd.count()}; time = $time ***********")
        rdd.foreachPartition(iter => {
          iter.map(line => {
            val split = line.value.split(",")
            val msg = new ProducerRecord[String, String](PUSH_TOPIC, split.mkString("|"));
            //发送消息
            producer.send(msg)
          })
        })
      }
    }

    ssc.start()
    ssc.awaitTermination()

    }

  def getKafkaConsumerParameters(groupId: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "linux121:9092,linux122:9092,linux123:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:
        java.lang.Boolean)
    )
  }

}
