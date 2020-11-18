package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class Source_Zidingyi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new CustomerSource());
        source.print();
        env.execute("Source_Zidingyi");
    }

    private static class CustomerSource implements SourceFunction<SensorReading> {


        Random random = new Random();
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sct) throws Exception {
            HashMap<String, Double> tempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                tempMap.put("sensor_" + i, 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String id : tempMap.keySet()) {
                    Double temp = tempMap.get(id);
                    double newTemp = temp + random.nextGaussian();
                    sct.collect(new SensorReading(id, System.currentTimeMillis(), newTemp));
                    tempMap.put(id, newTemp);
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}