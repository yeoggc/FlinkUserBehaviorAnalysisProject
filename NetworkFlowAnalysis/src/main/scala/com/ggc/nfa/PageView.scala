package com.ggc.nfa

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 商品点击量(窗口操作的输出类型)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object PageView extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
  env.setParallelism(1)

  val ds1 =
    env
      //      .socketTextStream("localhost", 7777)
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
      .map(line => {
        val split = line.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt, split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)



}
