package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

public class TimeWindow_EventTime_Late {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = input.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = map.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideoutput") {
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        sum.print("main");

        DataStream<Tuple2<String, Integer>> sideoutput = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideoutput"){});

        sideoutput.print("sideoutput");

        env.execute();
    }
}
