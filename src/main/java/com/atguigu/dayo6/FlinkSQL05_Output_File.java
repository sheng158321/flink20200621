package com.atguigu.dayo6;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class FlinkSQL05_Output_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<SensorReading> sensorDS = socketDS.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        Table table = tableEnv.fromDataStream(sensorDS);
        Table tableResult = table.select("id,temp").filter("id = 'sensor_1'");

        tableEnv.connect(new FileSystem().path("sensorOutput"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorOutput");

//        tableEnv.insertInto("sensorOutput1",sqlResult);
        tableEnv.insertInto("sensorOutput",tableResult);

        env.execute();

//        //1.获取执行环境并设置并行度
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        //2.读取文本数据创建流
//        DataStreamSource<String> readTextFile = env.socketTextStream("hadoop102",7777);
//
//        //3.将每一行数据转换为JavaBean
//        SingleOutputStreamOperator<SensorReading> sensorDataStream = readTextFile.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
//        });
//
//        //4.创建TableAPI执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        //5.从流中创建表
//        Table table = tableEnv.fromDataStream(sensorDataStream);
//
//        //6.转换数据
//        Table result = table.select("id,temp").filter("id = 'sensor_1'");
//
//        //7.将数据写入本地文件系统
//        tableEnv.connect(
//                new FileSystem().path("output/out.txt")) //定义文件系统连接,文件不能存在
//                .withFormat(new Csv()) // 定义格式化方法，Csv格式
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("temp", DataTypes.DOUBLE())
//                ) // 定义表结构
//                .createTemporaryTable("outputTable"); // 创建临时表
//
//        tableEnv.insertInto("outputTable", result);
//
//        //8.执行任务
//        env.execute();
    }
}
