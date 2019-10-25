package com.ggc.ma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 定义源数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketingViewCount(startTs: String, endTs: String, channel: String, behavior: String, count: Int)


object AppMarketingByChannelApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)


  val inputDs =
    env
      .addSource(new SimulatedEventSource)
//      .assignAscendingTimestamps(_.timestamp)
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MarketingUserBehavior](Time.seconds(60)) {
      override def extractTimestamp(element: MarketingUserBehavior): Long = {
        element.timestamp
      }
    })

  val ds1 =
    inputDs
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
//        println(s"inputDS.map data.timestamp=${data.timestamp}")
        ((data.channel, data.behavior), data.timestamp)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())
      .print()

  env.execute(getClass.getSimpleName)

}

class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[((String, String), Long)],
                       out: Collector[MarketingViewCount]): Unit = {

    val startTs = context.window.getStart
    val endTs = context.window.getEnd

    val channel = key._1
    val behavior = key._2

    val count = elements.size


    println(s"AppMarketingByChannelApp.MarketingCountByChannel#process elementsLast=${formatTs(elements.last._2)},watermark=${formatTs(context.currentWatermark)},startTs=${formatTs(startTs)},endTs=${formatTs(endTs)},channel=${channel},behavior=${behavior},elements.size=${count}")

    out.collect(MarketingViewCount(formatTs(startTs), formatTs(endTs), channel, behavior, count))

  }

  private def formatTs(ts: Long) = {
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }

}

