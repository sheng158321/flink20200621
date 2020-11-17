package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Wordcount_Unbounded {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> lineDS = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        result.print();

        env.execute("Wordcount_Unbounded");
    }
}
