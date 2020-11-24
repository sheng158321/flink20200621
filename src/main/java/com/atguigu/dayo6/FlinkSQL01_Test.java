package com.atguigu.dayo6;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

public class FlinkSQL01_Test {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据创建流
        DataStreamSource<String> input = env.readTextFile("sensor");

        //3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]));
        });

        //4.创建TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //6.转换数据
        //6.1 使用TableAPI转换数据
        Table table = tableEnv.fromDataStream(sensorDS);
        Table tableResult = table.select("id,temp").filter("id='sensor_1'");

        //6.2 使用FlinkSQL转换数据
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id = 'sensor_1");

        //7.转换为流输出数据
        tableEnv.toAppendStream(tableResult, Row.class).print();
        tableEnv.toAppendStream(sqlResult, Row.class).print();

        //8.启动任务
        env.execute();
    }
}
