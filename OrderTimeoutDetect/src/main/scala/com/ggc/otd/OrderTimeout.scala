package com.ggc.otd

import java.util

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 输入数据样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 输出订单支付结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val orderEventDS =
    env
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

  // 定义一个带匹配时间窗口的模式
  val orderPayPattern =
    Pattern
      .begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay").where(_.txId.nonEmpty)
      .within(Time.minutes(15))

  val patternStream = CEP.pattern(orderEventDS, orderPayPattern)


  val orderTimeoutOutputTag = OutputTag[OrderResult]("orderTimeout")

  val completedResult =
    patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

  completedResult.print("pay")
  completedResult.getSideOutput(orderTimeoutOutputTag).print("timeout")

  env.execute(getClass.getSimpleName)

}

class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {

    val payedOrderId =
      pattern.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "success")

  }
}


class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]],
                       timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId =
      pattern.get("begin").iterator().next().orderId

    OrderResult(timeoutOrderId, "time out")
  }
}
