package com.atguigu.dayo6;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkSQL04_Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "consumerkafka"))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.BIGINT()))
                .createTemporaryTable("kafkaTable");

        Table kafkaTable = tableEnv.from("kafkaTable");

        //4.TableAPI
        Table tableResult = kafkaTable.where("id='sensor_1'").select("id,temp");

        //5.SQL
//        Table sqlResult = tableEnv.sqlQuery("select id,temp from kafkaInput where id ='sensor_1'");

        //6.将结果数据打印
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
//        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //7.执行
        env.execute();
    }
}
