package com.atguigu.test;

import akka.stream.impl.fusing.Split;
import com.sun.deploy.trace.FileTraceListener;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.hash.Hash;
import scala.reflect.internal.Kinds;

import java.util.HashSet;

public class Distinct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDS = env.readTextFile("input");

        SingleOutputStreamOperator<String> flatMaps = lineDS.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] fields = s.split(" ");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });


        SingleOutputStreamOperator<String> filter = flatMaps.filter(new FilterFunction<String>() {
            HashSet<String> words = new HashSet<>();

            @Override
            public boolean filter(String s) throws Exception {
                boolean exist = words.contains(s);
                if (!exist) words.add(s);
                return !exist;
            }
        });

        filter.print();
        env.execute();
    }
}
