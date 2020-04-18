package com.ggc.hot_items_analysis

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 商品点击量(窗口操作的输出类型)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
  env.setParallelism(1)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "dw1:9092")
  properties.setProperty("group.id", "consumer-group")
  properties.setProperty("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")

  val ds1 =
    env
//      .socketTextStream("localhost", 7777)
            .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/UserBehavior.csv")
      //      .addSource(new FlinkKafkaConsumer[String]("Flink_Hot_Items", new SimpleStringSchema(), properties))
      .map(line => {
        val split = line.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt, split(3), split(4).toLong)
      })

  // 指定timestamp和watermark生成规则
  val ds2: DataStream[ItemViewCount] =
    ds1
      //    .assignAscendingTimestamps(_.timestamp * 1000)
      //      .assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(1))
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
          override def extractTimestamp(element: UserBehavior): Long = {
            element.timestamp * 1000L
          }
        })
      // 过滤出点击事件
      .filter(_.behavior == "pv")
      // 对商品id进行分组
      .keyBy("itemId")
      // 对每个商品id做滑动窗口（1小时窗口，5分钟滑动一次）
      .timeWindow(Time.hours(1), Time.minutes(5)) // WindowedStream[UserBehavior, Tuple(itemId), TimeWindow]
      .aggregate(new CountAgg, new MyWindowResultFunction) // DataStream[ItemViewCount]

  val ds3 =
    ds2
      .keyBy("windowEnd")
    .process(new TopNHotItem(3))



  //  ds1.print("Data")

  //  ds2.print("HotItems")

//  ds3.print("Final Result")

  env.execute(getClass.getSimpleName)

}


