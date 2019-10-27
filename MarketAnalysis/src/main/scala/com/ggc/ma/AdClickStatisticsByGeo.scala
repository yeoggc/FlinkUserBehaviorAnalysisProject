package com.ggc.ma

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdCountByProvince(windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdClickStatisticsByGeo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val blackListOutputTag = new OutputTag[BlackListWarning]("blackListOutputTag")

  val inputDS =
    env
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/AdClickLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


  // 添加黑名单过滤的逻辑
  val filterBlackListDS =
    inputDS
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListProcessKeyedFunction(100))


  val adCountDS =
    filterBlackListDS
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg, new AdCountResultWindow)

  adCountDS.print()

  filterBlackListDS
    .getSideOutput(blackListOutputTag)
    .print("BlackList")


  env.execute(getClass.getSimpleName)


}


class FilterBlackListProcessKeyedFunction(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  // 定义状态，保存用户对广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

  // 标识位，标记是否发送过黑名单信息
  lazy val isSend: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))

  // 保存定时器触发的时间戳
  lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state", classOf[Long]))


  override def processElement(value: AdClickLog,
                              ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context,
                              out: Collector[AdClickLog]): Unit = {

    // 获取当前的count值
    val curCount = countState.value()

    // 判断如果是第一条数据，count值是0,就注册一个定时器
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * 24 * 60 * 60 * 1000L
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTime.update(ts)
    }

    countState.update(curCount + 1)

    // 判断计数是否超出上限，如果超过输出黑名单信息到侧输出流
    if (curCount >= maxCount) {
      // 判断如果没有发送过黑名单信息，就输出
      if (!isSend.value()) {
        // 判断如果没有发送过黑名单信息，就输出
        //noinspection FieldFromDelayedInit
        ctx.output(AdClickStatisticsByGeo.blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today"))
        isSend.update(true)
      }
    } else {
      out.collect(value)
    }

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Long, Long),
                         AdClickLog, AdClickLog]#OnTimerContext,
                       out: Collector[AdClickLog]): Unit = {
    // 如果当前定时器是重置状态定时器，那么清空相应状态
    if (timestamp == resetTime.value()) {
      isSend.clear()
      countState.clear()
    }

  }

}

class CountAgg extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResultWindow extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[AdCountByProvince]): Unit = {

    val windowEnd = new Timestamp(window.getEnd).toString

    out.collect(AdCountByProvince(windowEnd, key, input.iterator.next()))

  }
}