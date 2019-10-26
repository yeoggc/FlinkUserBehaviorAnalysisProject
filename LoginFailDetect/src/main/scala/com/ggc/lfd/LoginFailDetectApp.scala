package com.ggc.lfd

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object LoginFailDetectApp extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(1L)

  val loginEventStream =
    env
      .readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/LoginLog.csv")
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
      //      .process(new LoginFailWarning(3))
      .process(new LoginFailWarningAdv(3))

  loginEventStream.print()
  env.execute(getClass.getSimpleName)


}


//noinspection DuplicatedCode
class LoginFailWarningAdv(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义一个List状态，用户保存连续登录失败的事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state", classOf[LoginEvent]))


  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {
    // 按照status筛选失败的事件，如果成功状态清空
    if (value.eventType == "fail") {
      // 定义迭代器获取状态
      val iter = loginFailListState.get().iterator()
      // 如果已经有失败事件才做处理，没有的话把当前事件直接add进去
      if (iter.hasNext) {
        val firstFailEvent = iter.next()
        //如果两次登录失败事件间隔小于2秒，输出报警信息
        if ((value.eventTime - firstFailEvent.eventTime).abs < 5) {
          out.collect(Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "login fail in 2 seconds"))
        }
        loginFailListState.clear()
        loginFailListState.add(value)

      } else {
        loginFailListState.add(value)
      }

    } else {
      loginFailListState.clear()
    }


  }
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
      println(s"LoginFailWarning#processElement eventtime=${value.eventTime * 1000L} , watermark=${ctx.timerService().currentWatermark()}")
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
