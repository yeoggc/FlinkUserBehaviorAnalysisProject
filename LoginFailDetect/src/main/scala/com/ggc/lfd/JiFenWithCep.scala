package com.ggc.lfd

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//noinspection DuplicatedCode
object JiFenWithCep extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // 1. 读取数据源
  //  val dataStream = env.readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/LoginLog.csv")
  val dataStream = env.socketTextStream("localhost", 7777)
    .map(data => {
      val dataArray = data.split(",")
      JiFenEvent(dataArray(0).toLong, dataArray(1).toInt, dataArray(2), dataArray(3).toLong)
    })
    .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[JiFenEvent](Time.seconds(3)) {
        override def extractTimestamp(element: JiFenEvent): Long =
          element.eventTime * 1000L
      })
    .keyBy(_.userId)

  // 2. 定义一个模式
  val loginFailPattern =
    Pattern
      .begin[JiFenEvent]("start", AfterMatchSkipStrategy.skipPastLastEvent())
      .where(x => true)
      .next("next")
      .timesOrMore(1)
      .where((event, ctx) => {

        val start = ctx.getEventsForPattern("start")
        println(s"start = ${start}")
        val next = ctx.getEventsForPattern("next")
        println(s"next = ${next}")


        var sum = 0
        val startIt = start.iterator
        val nextIt = next.iterator
        while (startIt.hasNext ) {
          sum += startIt.next().jifen
        }
        sum > 600
      })
      .within(Time.seconds(10))

  // 3. 将模式应用到数据流上
  val patternStream =
    CEP.pattern(dataStream, loginFailPattern)

  // 4. 从pattern stream中检出符合规则的事件序列做处理
  val loginFailWarningStream =
    patternStream.select(new LoginFailDetect)

  loginFailWarningStream.print()

  env.execute(getClass.getSimpleName)

  class LoginFailDetect extends PatternSelectFunction[JiFenEvent, Warning] {
    override def select(map: util.Map[String, util.List[JiFenEvent]]): Warning = {
      val firstFailEvent = map.get("start").iterator().next()

      Warning(
        firstFailEvent.userId,
        firstFailEvent.eventTime, 0L,
        "login fail 2 times")

    }
  }

  case class JiFenEvent(userId: Long, jifen: Int, eventType: String, eventTime: Long)


}



