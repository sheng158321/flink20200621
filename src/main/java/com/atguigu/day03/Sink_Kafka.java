package com.atguigu.day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sensor = env.readTextFile("sensor");

        sensor.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092", "test", new SimpleStringSchema()));

        env.execute();
    }
}
