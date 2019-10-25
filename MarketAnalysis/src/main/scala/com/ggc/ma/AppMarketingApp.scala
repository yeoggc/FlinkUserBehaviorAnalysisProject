package com.ggc.ma

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val inputDS =
    env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

  inputDS
    .filter(_.behavior != "UNINSTALL")
    .map(data => {
      ("dummyKey", 1L)
    })
    .keyBy(_._1)
    .timeWindow(Time.hours(1), Time.seconds(1))
    .process(new MarketingCountTotal())
    .print()

  env.execute(getClass.getSimpleName)

}

class MarketingCountTotal extends ProcessWindowFunction[(String, Long), MarketingViewCount, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[MarketingViewCount]): Unit = {

    val startTs = new Timestamp(context.window.getStart)
    val endTs = new Timestamp(context.window.getEnd)
    out.collect(MarketingViewCount(startTs.toString, endTs.toString, "total channel", "total behavior", elements.size))


  }
}