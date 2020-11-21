package com.atguigu.test;

import com.atguigu.day01.WordCount_Batch;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.print.Book;

public class TimeWindowSlide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> input = env.readTextFile("input");
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new WordCount_Batch.MyFlatMapFunc());
        //重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        keyedStream.print("groupbykey");

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5));

//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        SingleOutputStreamOperator<Integer> apply = window.apply(new WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {

            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
                out.collect(IteratorUtils.toList(input.iterator()).size());
            }
        });

        apply.print();

        env.execute();
    }
}
