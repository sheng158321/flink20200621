package com.atguigu.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sensor = env.readTextFile("sensor");
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        ElasticsearchSink<String> build = new ElasticsearchSink.Builder<>(httpHosts, new MySinkFunc()).build();
        sensor.addSink(build);
        env.execute();
    }

    private static class MySinkFunc implements ElasticsearchSinkFunction<String> {
        @Override
        public void process(String data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            String[] words = data.split(",");
            HashMap<Object, Object> hash = new HashMap<>();
            hash.put("id", words[0]);
            hash.put("ts", words[1]);
            hash.put("temp", words[2]);
            IndexRequest source = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .source(hash);
            requestIndexer.add(source);
        }
    }
}
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> inputDS = env.readTextFile("sensor");
//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop102",9200));
//
//        ElasticsearchSink<String> elasticSearchSink = new ElasticsearchSink.Builder<>(httpHosts, new MyEsSinkFunc())
//                .build();
//
//        inputDS.addSink(elasticSearchSink);
//        env.execute();
//    }
//
//    private static class MyEsSinkFunc implements ElasticsearchSinkFunction<String> {
//        @Override
//        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//
//            //对元素分割处理
//            String[] fields = element.split(",");
//
//            //创建Map用于存放待存储到ES的数据
//            HashMap<String, String> source = new HashMap<>();
//            source.put("id", fields[0]);
//            source.put("ts", fields[1]);
//            source.put("temp", fields[2]);
//
//            //创建IndexRequest
//            IndexRequest indexRequest = Requests.indexRequest()
//                    .index("sensor")
//                    .type("_doc")
////                    .id(fields[0])
//                    .source(source);
//
//            //将数据写入
//            indexer.add(indexRequest);
//
//        }
//    }