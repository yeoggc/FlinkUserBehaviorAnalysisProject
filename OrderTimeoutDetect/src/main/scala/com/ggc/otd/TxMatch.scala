package com.ggc.otd

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

//noinspection DuplicatedCode
object TxMatch extends App {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val orderEventDS =
    env
        .socketTextStream("localhost",7777)
//      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

  val receiptEventStream =
    env
      .socketTextStream("localhost",7778)
//      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/ReceiptLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

  val processedDS =
    orderEventDS.connect(receiptEventStream)
      .process(new TxMatchDetection)


  processedDS.print()

  processedDS.getSideOutput(unmatchedPays).print("unmatchedPays")
  processedDS.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")



  env.execute(getClass.getSimpleName)



  //noinspection DuplicatedCode
  class TxMatchDetection extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    lazy val payState: ValueState[OrderEvent] =
      getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] =
      getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {


      val receipt = receiptState.value()

      if (receipt != null) {
        //如果已经有receipt到了，那么正常输出匹配
        out.collect((value, receipt))

        receiptState.clear()
      } else {
        // 如果receipt还没到，那么保存pay状态，注册一个定时器等待
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)

      }
    }

    override def processElement2(value: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      val pay = payState.value()

      if (pay != null) {
        //如果已经有pay到了，那么正常输出匹配
        out.collect((pay, value))

        payState.clear()
      } else {
        // 如果receipt还没到，那么保存pay状态，注册一个定时器等待
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 3000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      println(s"onTimer =>> ${timestamp}")
      if(payState.value() != null){
        // 如果payState没有被清空，说明对应的receipt没到
        ctx.output(TxMatch.unmatchedPays,payState.value())
      }

      if(receiptState.value() != null){
        // 如果receiptState没有被清空，说明对应的pay没到
        ctx.output(TxMatch.unmatchedReceipts,receiptState.value())
      }

      payState.clear()
      receiptState.clear()

    }

  }


}

