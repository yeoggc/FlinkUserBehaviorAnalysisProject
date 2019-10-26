package com.ggc.lfd

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object LoginFailDetectApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(1L)

  val loginEventStream =
    env
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/LoginLog2.csv")
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
          override def extractTimestamp(element: LoginEvent): Long = {
            element.eventTime * 1000L
          }
        })
      .keyBy(_.userId)
      .process(new LoginFailWarning(3))

  loginEventStream.print()
  env.execute(getClass.getSimpleName)


}


class LoginFailWarning(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义一个ListState，用于保存连续登录失败的事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {

    // 判断是否是失败事件，如果是，添加到状态中，定义一个定时器
    if (value.eventType == "fail") {
      loginFailListState.add(value)
      println(s"LoginFailWarning#processElement eventtime=${value.eventTime*1000L} , watermark=${ctx.timerService().currentWatermark()}")
//            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 2000L)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
    } else {
      loginFailListState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 判断状态列表中登录失败的个数

    import scala.collection.JavaConversions._
    val times = loginFailListState.get().size

    if (times >= failTimes) {
      out.collect(Warning(ctx.getCurrentKey,
        loginFailListState.get().head.eventTime,
        loginFailListState.get().last.eventTime,
        "Login fail in 2 seconds for " + times + " times"))
    }

    // 清空状态
    loginFailListState.clear()

  }

}
