package com.ggc.hot_items_analysis

import org.apache.flink.api.common.functions.AggregateFunction

// COUNT统计的聚合函数实现，每出现一条记录就加一
class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = {
//    println("CountAgg createAccumulator")
    0L
  }

  override def add(value: UserBehavior, accumulator: Long): Long = {
//    println(s"CountAgg add input = ${value} , accumulator = ${accumulator}")
    accumulator + 1
  }

  override def getResult(accumulator: Long): Long = {
//    println(s"CountAgg getResult accumulator = ${accumulator}")
    accumulator
  }

  override def merge(acc1: Long, acc2: Long): Long = {
//    println(s"CountAgg merge  acc1 = ${acc1} , acc2 = ${acc2}")
    acc1 + acc2
  }
}
