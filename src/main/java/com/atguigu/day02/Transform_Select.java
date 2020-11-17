package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class Transform_Select {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

//        SplitStream<SensorReading> splitStream = sensorDS.split(new OutputSelector<SensorReading>() {
//
//            @Override
//            public Iterable<String> select(SensorReading value) {
//                return (value.getTemp() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
//            }
//        });

    }
}
