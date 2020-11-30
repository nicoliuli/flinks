package com.wb.day07;

import com.wb.common.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
    PurgingTrigger使用场景：
    在整个窗口中，每个2s触发窗口计算，每次都会清空状态。
    由于输出的result属于apend模式，所以如果落库到mysql，求sum即可

 */
public class PurgingTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fromSocket(env);
        env.execute("taigger job");
    }

    private static void fromSocket(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), "aaa", System.currentTimeMillis());
                return orderEvent;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTime();
            }
        }).timeWindowAll(Time.seconds(10)).trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.seconds(2)))).process(new ProcessAllWindowFunction<OrderEvent, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<OrderEvent> elements, Collector<String> out) throws Exception {
                Iterator<OrderEvent> iterator = elements.iterator();
                Long sum = 0L;
                while (iterator.hasNext()) {
                    sum += iterator.next().getOrderId();
                }
                out.collect("当前窗口：sum = " + sum);
            }
        }).print();
    }
}
