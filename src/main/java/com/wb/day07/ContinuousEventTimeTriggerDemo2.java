package com.wb.day07;

import com.wb.common.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
    ContinuousEventTime:
    案例中，在10s中的窗口中，每3s中触发一次窗口计算
    每次触发窗口计算，并不会清空状态，只有在窗口时间结束后，才会清空本窗口的状态

    ContinuousEventTime应用场景：要求在一个窗口内，每隔一段时间，输出最新结果
 */
public class ContinuousEventTimeTriggerDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fromSocket(env);
        env.execute("trigger job");
    }

    private static void fromSocket(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]), "aaa", Long.parseLong(split[1]));
                return orderEvent;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTime() * 1000;
            }
        }).timeWindowAll(Time.seconds(30), Time.seconds(15))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<OrderEvent, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<OrderEvent> elements, Collector<String> out) throws Exception {
                        Iterator<OrderEvent> iterator = elements.iterator();
                        Long sum = 0L;
                        while (iterator.hasNext()) {
                            sum += iterator.next().getOrderId();
                        }
                        out.collect("当前窗口：sum = " + sum + ",窗口是：" + context.window().getEnd() / 1000);
                    }
                }).print();
    }
}
