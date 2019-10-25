package com.ggc.nfa

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNHotUrls(val topSize: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {

  private var urlState:ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {

    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState-state",classOf[UrlViewCount]))

  }

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {

    urlState.add(value)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    // 获取收到的所有URL访问量
    val allUrlViews:ListBuffer[UrlViewCount] =  ListBuffer()

    import scala.collection.JavaConversions._

    for(urlView <- urlState.get()){
      allUrlViews += urlView
    }

    // 提前清除状态中的数据，释放空间
    urlState.clear()

    // 按照访问量从大到小排序
    val sortedUrlViews  = allUrlViews.sortBy(_.count)( Ordering.Long.reverse).take(topSize)

    // 将排名信息格式化成 String, 便于打印
    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)

  }

}
