package com.ggc.hot_items_analysis

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyAssignerWithPeriodicWatermarks(val delayTime:Long) extends AssignerWithPeriodicWatermarks[UserBehavior]{
  var ts :Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(ts - delayTime)
  }

  override def extractTimestamp(element: UserBehavior, previousElementTimestamp: Long): Long = {
    val ts = element.timestamp * 1000L
    this.ts = ts
    ts
  }
}
