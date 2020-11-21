package com.wb.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 测试水位线的推进、测输出流
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 事件时间

        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        OutputTag outputTag = new OutputTag<Tuple2<String,Long>>("late"){};
        WindowedStream windowedStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                Tuple2 t = new Tuple2();
                t.f0 = "device";
                t.f1 = Long.parseLong(value); // 时间 s
                return t;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.milliseconds(10)) {
            // 周期性生成watermark
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1 * 1000; // 提取时间戳,如果传入的时间信息不是ms，需要转化成ms
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).allowedLateness(Time.seconds(5)).sideOutputLateData(outputTag);


        SingleOutputStreamOperator reduce = windowedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String,Long> t2) throws Exception {
                return t1;
            }
        }, new TextProcessWindowFunction());
        reduce.print("result");
        reduce.getSideOutput(outputTag).print("late");
    }
}

// in out key time
class TextProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
        Tuple2<String, Long> next = elements.iterator().next(); // 返回聚合的数据
        String keyby = ((Tuple1<String>) key).f0;
        out.collect(next);
    }
}
