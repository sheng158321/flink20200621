package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Richmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        SingleOutputStreamOperator map = sensorDS.map(new MyRichMapFunc());

        map.print();
        env.execute();
    }

    private static class MyRichMapFunc extends RichMapFunction<String, SensorReading> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public SensorReading map(String str) throws Exception {
            String[] split = str.split(",");
            return new SensorReading(split[0],
                    Long.parseLong(split[1]),
                    Double.parseDouble(split[2]));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
