package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<SensorReading> map = sensorDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = map.keyBy("id");

        SingleOutputStreamOperator<SensorReading> maxResult = keyedStream.max("temp");
        SingleOutputStreamOperator<SensorReading> maxbyResult = keyedStream.maxBy("temp");

        maxResult.print();
        maxbyResult.print();

        env.execute();
    }
}
