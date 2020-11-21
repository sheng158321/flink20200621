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
import org.apache.flink.util.OutputTag;

public class ProcessAPI_Sideoutput {
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

        SingleOutputStreamOperator<SensorReading> process = id.process(new MyProcessFunc());

        process.print();

        env.execute();
    }

    private static class MyProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, SensorReading> {
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            Double temp = value.getTemp();

            if (temp > 30.0D) {
                //高温数据,发送数据至主流
                out.collect(value);
            } else {
                //低温数据,发送数据至低温流
                ctx.output(new OutputTag<String>("low"){}, value.getId());
            }
        }
    }
}
