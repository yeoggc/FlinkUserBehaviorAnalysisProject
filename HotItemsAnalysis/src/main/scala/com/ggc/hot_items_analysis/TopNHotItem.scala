package com.ggc.hot_items_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 求某个窗口中前N名的热门点击商品，key为商品时间戳，输出位TopN的结果字符串
class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 命名状态变量的名字和状态变量的类型
    val itemsStateDesc =
      new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])

    //从运行时上下文中获取状态并赋值
    itemState = getRuntimeContext.getListState(itemsStateDesc)

  }

  override def processElement(input: ItemViewCount,
                              ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {

    //每天数据都保存在状态中
    itemState.add(input)

//    println(s"TopNHotItem#processElement input=${input} ")

    // 注册windowEnd + 1的 EventTime Timer，当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    // 也就是当程序看到windowEnd + 1 的水位线时，触发onTimer回调函数
    ctx.timerService().registerEventTimeTimer(input.windowEnd + 1)


  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {



    // 获取收到的所有商品点击量
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._

    for (item <- itemState.get) {
      allItems += item
    }

//    println(s"TopNHotItem#onTimer allItems=${allItems} ")

    // 提交清除状态中的数据，释放空间
    itemState.clear()

    // 按照点击量从大到小排序
    val sortedItems: ListBuffer[ItemViewCount] =
      allItems
        .sortBy(_.count)(Ordering.Long.reverse)
        .take(topSize)

    // 将排名信息格式转化成String,便于打印
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }

    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)

    out.collect(result.toString())

  }

}
