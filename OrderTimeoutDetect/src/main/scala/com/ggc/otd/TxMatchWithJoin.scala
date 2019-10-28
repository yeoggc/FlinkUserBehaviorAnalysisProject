package com.ggc.otd

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


//noinspection DuplicatedCode
object TxMatchWithJoin extends App {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val orderEventDS =
    env
      //      .socketTextStream("localhost", 7777)
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)

  val receiptEventStream =
    env
      //      .socketTextStream("localhost", 7778)
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/ReceiptLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)

  //  orderEventDS.join(receiptEventStream)
  //    .where(_.txId)
  //    .equalTo(_.txId)
  //    .window(TumblingEventTimeWindows.of(Time.seconds(15)))
  //    .apply((pay, receipt) => {
  //      (pay, receipt)
  //    })
  //    .print()

  orderEventDS
    .keyBy(_.txId).intervalJoin(receiptEventStream.keyBy(_.txId))
    .between(Time.seconds(-15), Time.seconds(20))
    .process(new TxMatchByIntervalJoin())
      .print()

  env.execute(getClass.getSimpleName)

}

class TxMatchByIntervalJoin extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    out.collect((left, right))

  }
}