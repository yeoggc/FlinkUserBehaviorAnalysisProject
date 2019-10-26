package com.ggc.ma

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdCountByProvince(windowEnd: String, province: String, count: Long)

object AdClickStatisticsByGeo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


  val inputDS =
    env
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/AdClickLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)

  val adCountDS =
    inputDS
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg, new AdCountResultWindow)

  adCountDS.print()


  env.execute(getClass.getSimpleName)


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