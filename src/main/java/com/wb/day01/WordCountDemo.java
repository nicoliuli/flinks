package com.wb.day01;

import com.wb.common.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordCount,从网络流输入数据，统计并输出
 */
public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(8);
        wordCountProcess(env);
        env.execute("Word Count Job Exec");
    }

    // nc -lk 8888命令开启网络输入
    public static void wordCountProcess(StreamExecutionEnvironment env) throws Exception {
        env.socketTextStream("localhost", 8888).flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(new WordCount(word,1));
                }
            }
        }).keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount wc) throws Exception {
                return wc.getWord(); // word字段为分组字段
            }
        }).reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                WordCount wc = new WordCount();
                wc.setWord(wc1.getWord());
                wc.setCount(wc1.getCount()+wc2.getCount());
                return wc;
            }
        }).print();
    }

    // nc -lk 8888命令开启网络输入
    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        env.socketTextStream("localhost", 8888).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : value.split(" ")) {
                    collector.collect(new Tuple2(word, 1));
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2 reduce(Tuple2 t1, Tuple2 t2) throws Exception {
                Tuple2 t = new Tuple2();
                t.setField(t1.getField(0), 0);
                t.setField((Integer) t1.getField(1) + (Integer) t2.getField(1), 1);
                return t;
            }
        }).print();
    }

    // nc -lk 8888命令开启网络输入
    public static void testWordCount1(StreamExecutionEnvironment env) throws Exception {
        env.socketTextStream("localhost", 8888).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : value.split(" ")) {
                    collector.collect(new Tuple2(word, 1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0; // 第一个字段为分组key
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                Tuple2 t = new Tuple2();
                t.f0 = t1.f0;
                t.f1 = t1.f1 + t2.f1;
                return t;
            }
        }).print();
    }
}
