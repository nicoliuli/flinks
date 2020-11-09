package com.wb.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 事件时间
      //  env.getConfig().setAutoWatermarkInterval(200);// 周期性生成watermark，默认周期200ms
        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        String keyby[] = {"aaa","bbb"};
        WindowedStream windowedStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Tuple3<String,String, Long>>() {
            @Override
            public Tuple3<String,String, Long> map(String value) throws Exception {
                String[] s = value.split(" ");
                Tuple3 t = new Tuple3();
                t.f0 = keyby[new Random().nextInt(2)];
                t.f1 = s[0];
                t.f2 = System.currentTimeMillis();//Long.parseLong(s[1]);
                return t;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String,String, Long>>(Time.milliseconds(10)) { // 延迟10s（最大乱序程度）
            // 周期性生成watermark
            @Override
            public long extractTimestamp(Tuple3<String,String, Long> element) {
                return element.f2; // 提取时间戳,如果传入的时间信息不是ms，需要转化成ms
            }
        }).map(new MapFunction<Tuple3<String,String, Long>, Tuple3<String,String, Long>>() {
            @Override
            public Tuple3<String,String, Long> map(Tuple3<String,String, Long> value) throws Exception {
                return value;
            }
        }).keyBy(0).timeWindow(Time.seconds(5));
        SingleOutputStreamOperator reduce = windowedStream.reduce(new ReduceFunction<Tuple3<String,String, Long>>() {

            @Override
            public Tuple3<String,String, Long> reduce(Tuple3<String,String, Long> t1, Tuple3<String,String, Long> t2) throws Exception {
                return Integer.parseInt(t1.f1) > Integer.parseInt(t2.f1) ? t1 : t2;
            }
        },new MyProcessWindowFunction());

        reduce.print("result");


    }
}

// in out key time
class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String,String, Long> ,Tuple3<String,String, Long> , Tuple, TimeWindow>{

    @Override
    public void process(Tuple key, Context context, Iterable<Tuple3<String,String, Long>> elements, Collector<Tuple3<String,String, Long>> out) throws Exception {
        Tuple3<String,String, Long> next = elements.iterator().next(); // 返回聚合的数据
        String keyby = ((Tuple1<String>) key).f0;
        while (elements.iterator().hasNext()){
            System.out.println(elements.iterator().next());
        }
        out.collect(next);
    }
}
