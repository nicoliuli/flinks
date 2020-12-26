package com.wb.day03;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 广播状态
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        test(env);
        env.execute("job");
    }

    private static void test(StreamExecutionEnvironment env) {
        MapStateDescriptor<Integer, String> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", Integer.class, String.class);

        BroadcastStream<Tuple2<Integer, String>> broadcast = env.addSource(new RichSourceFunction<Tuple2<Integer, String>>() {
            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                String[] pattern = {"yyyy-MM", "yyyy-MM-dd", "yyyy年MM月"};
                while (true) {
                    for (String s : pattern) {
                        ctx.collect(new Tuple2<>(1, s));
                        Thread.sleep(200);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).broadcast(ruleStateDescriptor);


        DataStreamSource<Date> timeStream = env.addSource(new RichSourceFunction<Date>() {
            @Override
            public void run(SourceContext<Date> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Date());
                }
            }

            @Override
            public void cancel() {

            }
        });

        timeStream.connect(broadcast).process(new BroadcastProcessFunction<Date, Tuple2<Integer, String>, String>() {

            MapStateDescriptor<Integer, String> ruleStateDescriptor;

            @Override
            public void open(Configuration parameters) throws Exception {
                ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", Integer.class, String.class);
            }

            @Override
            public void processElement(Date value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String format = "";
                // 获取广播状态
                Iterable<Map.Entry<Integer, String>> entries = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();
                for (Map.Entry<Integer, String> entry : entries) {
                    if (entry.getKey() == 1) {
                        format = entry.getValue();
                    }
                }
                // 将广播状态应用于非广播流中
                if (format != null && !"".equals(format)) {
                    String sdf = new SimpleDateFormat(format).format(value);
                    out.collect(sdf);
                }
            }

            @Override
            public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                // 获取广播状态并更新状态
                BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                broadcastState.put(value.f0, value.f1);
            }

        }).print("输出：");


    }
}
