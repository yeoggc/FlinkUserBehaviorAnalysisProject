package com.ggc.hot_items_analysis

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 用于输出窗口的结果
class MyWindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggregateResult: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {


    val itemId:Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = aggregateResult.iterator.next()
    val itemViewCount = ItemViewCount(itemId,window.getEnd,count)
//    println(s"MyWindowResultFunction apply itemViewCount = ${itemViewCount}")

    out.collect(itemViewCount)

  }
}

class MyWindowResultFunction2 extends ProcessWindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def process(key: Tuple,
                       context: Context,
                       elements: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
    val window = context.window


  }
}