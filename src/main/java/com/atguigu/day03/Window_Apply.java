package com.atguigu.day03;

import com.atguigu.day01.WordCount_Batch;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Window_Apply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatmaps = socketTextStream.flatMap(new WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = flatmaps.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windows = keyed.timeWindow(Time.seconds(5));

//        SingleOutputStreamOperator<Integer> apply = windows.apply(new MyApplyFunc());

//        apply.print();

        SingleOutputStreamOperator<Integer> apply = windows.apply(new WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
                out.collect(IteratorUtils.toList(input.iterator()).size());
            }
        });

        apply.print();

        env.execute();
    }

    private static class MyApplyFunc implements WindowFunction<Tuple2<String, Integer>,
            Integer, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
//            Integer count = 0;
//            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
//            while (iterator.hasNext()){
//                Tuple2<String, Integer> next = iterator.next();
//                count+=1;
//            }
//            out.collect(count);

            out.collect(
                    IteratorUtils.toList(input.iterator()).size()
            );
        }
    }
}
