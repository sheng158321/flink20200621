package com.atguigu.day04;

import com.atguigu.day01.WordCount_Batch;
import javafx.scene.chart.BubbleChart;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class State_Wordcount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketTextStream.flatMap(new WordCount_Batch.MyFlatMapFunc());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> count = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private ValueState<Integer> countState = null;


//            @Override
//            public void open(Configuration parameters) throws Exception {
//                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count",
//                        Integer.class, 0));
//            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Integer count = countState.value();

                count++;
//                count += value.f1;

                countState.update(count);

                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count",
                        Integer.class, 0));

                return new Tuple2<>(value.f0, count);
            }
        });

        count.print();

        env.execute();

    }
}
