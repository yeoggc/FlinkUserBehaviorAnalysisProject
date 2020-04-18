package com.ggc.hot_items_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    writeToKafka("Flink_Hot_Items")
  }
  def writeToKafka(topic: String): Unit ={
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "dw1:9092")
//    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//
//    // 创建生产者
//    val producer = new KafkaProducer[String, String](properties)
//    // 从文件中读取数据
//    val bufferSource = io.Source.fromFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
//    for( line <- bufferSource.getLines() ){
//      val record = new ProducerRecord[String, String](topic, line)
//      producer.send(record)
//    }
//    producer.close()
  }
}
