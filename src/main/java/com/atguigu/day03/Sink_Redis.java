package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sensor = env.readTextFile("sensor");
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();
        sensor.addSink(new RedisSink<>(conf,new MyRedisSink()));
        env.execute();
    }

    private static class MyRedisSink implements RedisMapper<String>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(String data) {
            String[] split = data.split(",");
            return split[0];
        }

        @Override
        public String getValueFromData(String data) {
            String[] split = data.split(",");
            return split[2];
        }
    }
}
//public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> sensor = env.readTextFile("sensor");
//
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost("hadoop102")
//                .setPort(6379)
//                .build();
//
//        sensor.addSink(new RedisSink<String>(conf,new MyRedisMap()));
//    }
//
//    private static class MyRedisMap implements RedisMapper<String> {
//        @Override
//        public RedisCommandDescription getCommandDescription() {
//            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
//        }
//
//        @Override
//        public String getKeyFromData(String data) {
//            String[] fields = data.split(",");
//            return fields[0];
//        }
//
//        @Override
//        public String getValueFromData(String data) {
//            String[] fields = data.split(",");
//            return fields[2];
//        }
//    }