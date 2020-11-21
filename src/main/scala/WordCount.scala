package com.atguigu

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lineDS: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val words: DataStream[String] = lineDS.flatMap(_.split(" "))

    val maps: DataStream[(String, Int)] = words.map((_, 1))

    val keyedds: KeyedStream[(String, Int), Tuple] = maps.keyBy(0)

    val result: DataStream[(String, Int)] = keyedds.sum(1)

    result.print()
  }
}
