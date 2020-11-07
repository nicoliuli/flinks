package com.wb.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class SinkEsDemo {

    // 经测试，es里查不到，需要再看看
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        sinkEs(env);
        env.execute("Flink Streaming Java API Skeleton");
    }


    private static void sinkEs(StreamExecutionEnvironment env) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));
        ElasticsearchSink<String> esSink = new ElasticsearchSink.Builder<>(httpHosts, new MyEsFunc()).build();

        env.socketTextStream("localhost", 8888).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).addSink(esSink);
    }
}

class MyEsFunc implements ElasticsearchSinkFunction<String> {

    /**
     * @param input   表示输入，参数的类型要和上一个算子输出的类型匹配
     * @param ctx
     * @param indexer 调用es发http请求的
     */
    @Override
    public void process(String input, RuntimeContext ctx, RequestIndexer indexer) {
        Map<String, String> json = new HashMap<>();
        json.put("aaa", input);

        IndexRequest source = Requests.indexRequest()
                .index("my-index")
                //.type("my-type")
                .id(input).source(json);

        indexer.add(source); // 发送请求
    }
}
