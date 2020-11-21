package com.atguigu.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import sun.print.PeekGraphics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class Kafka_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Kafka配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        //读取Kafka数据
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties));

        kafkaDS.addSink(new MyJDBCFunc());
        env.execute();
    }

    private static class MyJDBCFunc extends RichSinkFunction<String> {

        Connection connection;
        PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            connection.prepareStatement("insert into table kafkamysql(id,name) values (?,?) on duplicate key update name =?");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] words = value.split(",");
            preparedStatement.setInt(1, Integer.parseInt(words[0]));
            preparedStatement.setString(2, words[1]);
            preparedStatement.setString(3, words[1]);
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
