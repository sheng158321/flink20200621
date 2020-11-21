package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataSource<String> lineDS = env.readTextFile("input");

        //压平
        FlatMapOperator<String, Tuple2<String, Integer>> wordsToOneDS = lineDS.flatMap(new MyFlatMapFunc());

        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordsToOneDS.groupBy(0);

        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        result.print();
    }

    //自定义flatmap方法
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            //按照空格切分value
            String[] words = value.split(" ");

            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
