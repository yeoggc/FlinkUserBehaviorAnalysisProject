package com.ggc.nfa

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 用于输出窗口的结果
class MyWindowResultFunction extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggregateResult: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {


    val url: String = key.asInstanceOf[Tuple1[String]].f0
    val count: Long = aggregateResult.iterator.next()
    val urlViewCount = UrlViewCount(url, window.getEnd, count)

    out.collect(urlViewCount)

  }
}

