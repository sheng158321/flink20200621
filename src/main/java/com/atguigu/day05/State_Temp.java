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
import org.apache.flink.util.Collector;

public class State_Temp {
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

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = keyed.flatMap(new MyStateTempFunc());

        result.print();

        env.execute();
    }

    private static class MyStateTempFunc extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,
            Double>> {

        private ValueState<Double> lastTempState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",
                    Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Double curTemp = value.getTemp();

            if(lastTempState != null && Math.abs(curTemp) > 10.0){
                out.collect(new Tuple3<>(value.getId(),lastTemp,curTemp));
            }

            lastTempState.update(curTemp);
        }
    }
}
