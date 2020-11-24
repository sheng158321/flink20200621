package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OperatingSystem;

public class State_OnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> map = input.map(line ->
                {
                    String[] split = line.split(",");
                    return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                }
        );

        KeyedStream<SensorReading, Tuple> keyed = map.keyBy("id");

        SingleOutputStreamOperator<String> result = keyed.process(new MyTempDescFunc());

        result.print();

        env.execute();
    }

    private static class MyTempDescFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {

        //定义状态
        private ValueState<Double> lastTempState = null;
        private ValueState<Long> tsState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",
                    Double.class));

            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //提取上一次温度值
            Double lastTemp = lastTempState.value();
            //上一次的时间
            Long lastTs = tsState.value();
            long ts = ctx.timerService().currentProcessingTime() + 5000L;

            if (lastTs == null) {
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            } else {
                if (lastTemp != null && value.getTemp() < lastTemp) {
                    ctx.timerService().deleteProcessingTimeTimer(tsState.value());
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }
            }

            lastTempState.update(value.getTemp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续十秒温度没有下降");
            tsState.clear();
        }
    }
}
