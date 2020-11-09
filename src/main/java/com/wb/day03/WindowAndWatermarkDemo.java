package com.wb.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowAndWatermarkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 事件时间
        env.getConfig().setAutoWatermarkInterval(200);// 周期性生成watermark，默认周期200ms
        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }


    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        OutputTag outputTag = new OutputTag<Tuple2<String,Long>>("late"){};
        WindowedStream windowedStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] s = value.split(" ");
                Tuple2 t = new Tuple2();
                t.f0 = s[0];
                t.f1 = System.currentTimeMillis();//Long.parseLong(s[1]);
                return t;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(1)) { // 延迟10s（最大乱序程度）
            // 周期性生成watermark
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1; // 提取时间戳,如果传入的时间信息不是ms，需要转化成ms
            }
        }).map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                return value;
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).allowedLateness(Time.seconds(1)).sideOutputLateData(outputTag);
        SingleOutputStreamOperator reduce = windowedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                Tuple2 t = new Tuple2();
                t.f0 = t1.f0;
                t.f1 = t2.f1;
                return t;
            }
        });

        reduce.getSideOutput(outputTag).print("late");
        reduce.print("result");


    }
}
