package com.atguigu.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WorCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = jsc.textFile("input");

        JavaRDD<String> wordsRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordToOneRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordSumRDD = wordToOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        for (
                Tuple2<String, Integer> tuple2 : wordSumRDD.collect()) {
            System.out.println(tuple2._1 + ":" + tuple2._2);
        }

        jsc.close();

    }
}
