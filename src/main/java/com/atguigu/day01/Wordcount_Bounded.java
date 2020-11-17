package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Wordcount_Bounded {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> lineDataStream = env.readTextFile("input");

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDataSteam = lineDataStream.flatMap(new WordCount_Batch.MyFlatMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = wordToOneDataSteam.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2TupleKeyedStream.sum(1);

        result.print();

        env.execute();
    }
}
