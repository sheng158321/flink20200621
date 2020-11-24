package com.atguigu.dayo6;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class FlinkSQL03_Csv {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensorInput");

        Table table = tableEnv.from("sensorInput");

        Table tableResult = table.select("id,temp").where("id='sensor_1");
        //sql
//        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensorInput where id = 'sensor_1");

        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
//        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        env.execute();
    }
}
