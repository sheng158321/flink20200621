package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessAPI_Ontime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");

        SingleOutputStreamOperator<String> process = id.process(new MyProcessFunc());

        process.print();

        env.execute();
    }

    private static class MyProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            TimerService timerService = ctx.timerService();
            long ts = timerService.currentProcessingTime();//当前处理时间
            timerService.registerProcessingTimeTimer(ts + 5000L);

            out.collect(value.getId());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("定时器工作了");
        }
    }
}
