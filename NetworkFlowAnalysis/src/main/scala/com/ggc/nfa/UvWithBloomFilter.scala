package com.ggc.nfa

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

//noinspection DuplicatedCode
object UvWithBloomFilter extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val inputStream = env.readTextFile("/Users/yeoggc/Documents/AtguiguCode/Flink/Flink_Project_Atguigu/FlinkUserBehaviorAnalysisProject/res/UserBehavior.csv")
    .map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
    })
    .assignAscendingTimestamps(_.timestamp * 1000L)
    .filter(_.behavior == "pv")
    .map(data => ("uv", data.userId))
    .keyBy(_._1)
    .timeWindow(Time.hours(1))
    .trigger(new MyTrigger)
    .process(new UvCountWithBloom)
    .print()


  env.execute(getClass.getSimpleName)

}

// 自定义窗口触发机制，每来一条数据就触发一次窗口操作，写入到redis中
class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long),
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {

    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  /**
   * 在事件时间语义下，定义了一些Timer，当触发这些Timer时，onEventTime方法会被调用
   *
   *
   * 默认的Trigger是EventTimeTrigger
   *
   */
  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {

    TriggerResult.CONTINUE // 表示window什么都不做
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

  }
}

// 定义一个布隆过滤器，要求传入的size是整数
class Bloom(bitSize: Long) extends Serializable {

  private val capital = bitSize

  // 用Hash函数实现userID到每一个位的对应关系
  def hash(value: String, seed: Int): Long = {

    //    MurmurHash3.stringHash(value)

    var result = 0
    for (i <- value.indices) {

      result = result * seed + value.charAt(i)
    }

    (capital - 1) & result
  }
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  // 定义redis连接和布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 28) // 32MB位图，存储2亿多个位

  override def process(key: String, context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[UvCount]): Unit = {

    // 在redis里存储位图，以windowEnd作为key存储
    val storeKey = context.window.getEnd.toString
    // 把当前窗口uv的count值也存入redis，存入一张hash表，表名叫count
    var count: Long = 0L

    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    // 根据hash值，查对应偏移量的位是否有值，说明当前user是否存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)

    val isExist = jedis.getbit(storeKey, offset)

    if (!isExist) {
      // 如果不存在，就将位图对应位置设置为1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    }

  }
}