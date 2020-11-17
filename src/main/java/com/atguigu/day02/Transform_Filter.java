package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Filter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        SingleOutputStreamOperator<String> filter = sensorDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                double temp = Double.parseDouble(value.split(",")[2]);
                return temp > 30.0D;
            }
        });

        filter.print();
        env.execute();
    }
}
