package com.ggc.nfa

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class UvCount(windowEnd: Long, count: Long)

//noinspection DuplicatedCode
object UniqueVisitor extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val inputDS =
    env.
      readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

  val aggDS =
    inputDS
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //设置1小时滚动窗口
      .apply(new UvCountByAllWindow)
      .print()

  env.execute(getClass.getSimpleName)
}

class UvCountByAllWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow,
                     input: Iterable[UserBehavior],
                     out: Collector[UvCount]): Unit = {
    // 用一个set来做去重
    var idSet: Set[Long] = Set[Long]()
    // 把每个数据放入Set
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }

    println(s"UvCountByAllWindow#apply windowEndTime=${window.getEnd}")

    //输出结果
    out.collect(UvCount(window.getEnd, idSet.size))

  }
}