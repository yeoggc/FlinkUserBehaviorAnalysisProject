package com.ggc.fc01_scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object SFlinkSimplestDemo01 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("aa","bb","cc")
      .map(value => (value,1))
      .print()

    env.execute(getClass.getSimpleName)

  }
}
