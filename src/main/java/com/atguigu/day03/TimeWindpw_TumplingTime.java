package com.atguigu.day03;

import com.atguigu.day01.WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TimeWindpw_TumplingTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> input = env.readTextFile("input");
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new WordCount_Batch.MyFlatMapFunc());
        //重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);
        //简化版滚动时间开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindowDS = keyedStream.timeWindow(Time.seconds(5));
//        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindowDS = keyedStream.timeWindow(Time.seconds(5),Time.seconds(2));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = timeWindowDS.sum(1);

        sum.print();

        env.execute();
    }
}
