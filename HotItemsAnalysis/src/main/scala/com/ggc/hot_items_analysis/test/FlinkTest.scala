package com.ggc.hot_items_analysis.test

import com.ggc.hot_items_analysis.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//noinspection DuplicatedCode
object FlinkTest extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
  env.setParallelism(1)



  val ds1 =
    env
      .socketTextStream("localhost", 7777)
      .map(line => {
        val split = line.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt, split(3), split(4).toLong)
      })

  // 指定timestamp和watermark生成规则
  val ds2 =
      ds1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(0)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
      })
      // 过滤出点击事件
      .filter(_.behavior == "pv")
      // 对商品id进行分组
      .keyBy("itemId")
      // 对每个商品id做滑动窗口（1小时窗口，5分钟滑动一次）
      .timeWindow(Time.hours(1), Time.minutes(5)) // WindowedStream[UserBehavior, Tuple(itemId), TimeWindow]
//      .aggregate(new CountAgg, new MyWindowResultFunction) // DataStream[ItemViewCount]
          .aggregate(new AggregateFunction[UserBehavior,Long,Long] {
            override def createAccumulator(): Long = 0L

            override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

            override def getResult(accumulator: Long): Long = accumulator

            override def merge(a: Long, b: Long): Long = a + b
          })



    ds1.print("Data")

    ds2.print("HotItems")

  env.execute(getClass.getSimpleName)
}
