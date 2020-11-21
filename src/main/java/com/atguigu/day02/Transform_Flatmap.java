package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transform_Flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<String> flatMaps = sensorDS.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        flatMaps.print();
        env.execute();
    }
}
