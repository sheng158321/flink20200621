package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sensor = env.readTextFile("sensor");
        sensor.addSink(new MyJdbcSink());
        env.execute();
    }

    private static class MyJdbcSink extends RichSinkFunction<String> {

        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            insertStmt = connection.prepareStatement("INSERT INTO sensor2 (id, temp) VALUES (?, ?)");
            updateStmt = connection.prepareStatement("UPDATE sensor2 SET temp = ? WHERE id = ?");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] words = value.split(",");

            updateStmt.setString(1,  words[2]);
            updateStmt.setString(2,words[0]);
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, words[0]);
                insertStmt.setString(2, words[2]);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> sensor = env.readTextFile("sensor");
//
//        sensor.addSink(new MyJdbcSink());
//
//        env.execute();
//    }
//
//    private static class MyJdbcSink extends RichSinkFunction<String> {
//
//        Connection connection = null;
//        PreparedStatement preparedStatement = null;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
//            preparedStatement = connection.prepareStatement("INSERT INTO sensor(id,temp) VALUES(?,?)" +
//                    "on DUPLICATE key update temp=?");
//        }
//
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            String[] fields = value.split(",");
//
//            preparedStatement.setString(1, fields[0]);
//            preparedStatement.setDouble(2, Double.parseDouble(fields[2]));
//            preparedStatement.setDouble(3, Double.parseDouble(fields[2]));
//
//            preparedStatement.execute();
//        }
//
//        @Override
//        public void close() throws Exception {
//            preparedStatement.close();
//            connection.close();
//        }
//    }
//}
//
