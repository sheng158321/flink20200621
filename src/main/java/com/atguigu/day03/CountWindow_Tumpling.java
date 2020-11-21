package com.atguigu.day03;

import com.atguigu.day01.WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class CountWindow_Tumpling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flat = socket.flatMap(new WordCount_Batch.MyFlatMapFunc());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = flat.keyBy(0);
//        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = keyed.countWindow(5);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = keyed.countWindow(5,2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        sum.print();
        env.execute();
    }
}
