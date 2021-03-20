package com.makoto.sparkStream
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KafkaProducer {
  val TOPIC = "topicA"
  val BROKERS = "linux121:9092,linux122:9092,linux123:9092"
  def main(args: Array[String]): Unit = {
    //define spark context
    val conf = new SparkConf().setMaster("local").setAppName("sprak stream kafka integration").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    // 定义 kafka 参数
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])

    //rdd from fetching the log txt file
    val data: RDD[String] = spark.textFile("data/mysample.log")

    // KafkaProducer
    val producer = new KafkaProducer[String, String](prop)
    data.foreach(line => {
      val sanitizedLine = line.replace("<<<!>>>", "")
      val msg = new ProducerRecord[String, String](TOPIC, sanitizedLine);
      producer.send(msg)
    })
    producer.close()
  }
}
