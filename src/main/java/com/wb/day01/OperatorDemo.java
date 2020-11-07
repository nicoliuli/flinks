package com.wb.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 算子练习
 * split select connect union算子
 */
public class OperatorDemo {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        rich(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static void splitSelectConnect(StreamExecutionEnvironment env){
        SplitStream<Integer> split = env.socketTextStream("localhost", 8888).flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                if(value==null || "".equals(value.trim())){
                    out.collect(1);
                }else{
                    out.collect(Integer.parseInt(value));
                }

            }
        }).split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer i) {
                List<String> list = new ArrayList<>();
                if (i % 2 == 0) {
                    list.add("ou");
                } else {
                    list.add("ji");
                }
                return list;
            }
        });

        DataStream<Integer> ou = split.select("ou");
        DataStream<Integer> ji = split.select("ji");

        // connect两个flataMap，两个流的数据类型可以不一样，返回的数据类型也可以不一样。union两个流的数据类型必须一样
        ou.connect(ji).map(new CoMapFunction<Integer, Integer, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                // 处理第一个流的数据
                return "a";
            }

            @Override
            public Object map2(Integer value) throws Exception {
                // 处理第二个流的数据
                return 1;
            }
        }).print();
    }

    public static void splitSelectUnion(StreamExecutionEnvironment env){
        SplitStream<Integer> split = env.socketTextStream("localhost", 8888).flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                if(value==null || "".equals(value.trim())){
                    out.collect(1);
                }else{
                    out.collect(Integer.parseInt(value));
                }

            }
        }).split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer i) {
                List<String> list = new ArrayList<>();
                if (i % 2 == 0) {
                    list.add("ou");
                } else {
                    list.add("ji");
                }
                return list;
            }
        });

        DataStream<Integer> ou = split.select("ou");
        DataStream<Integer> ji = split.select("ji");

        // union，两个流的数据类型必须一样，union是变长参数可以合并多个流
        ou.union(ji).print();
    }

    public static void rich(StreamExecutionEnvironment env){
        env.socketTextStream("localhost", 8888).flatMap(new RichFlatMapFunction<String, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open"); //生命周期方法，在生命周期内只会执行一次
            }

            @Override
            public void close() throws Exception {
                System.out.println("close"); //生命周期方法，在生命周期内只会执行一次
            }

            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                if(value==null || "".equals(value.trim())){
                    out.collect(1);
                }else{
                    out.collect(Integer.parseInt(value));
                }
            }
        }).print();
    }
}
