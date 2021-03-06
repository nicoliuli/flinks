package com.wb.day03;

import com.wb.common.OrderEvents;
import com.wb.common.ReceiptEvents;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 实时对账
public class TxMatchDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 定义测输出流，输出只有pay事件没有receipt事件的异常信息
        OutputTag payEventTag = new OutputTag<String>("payEventTag-side") {};
        // 定义测输出流，输出只有receipt事件没有pay事件的异常信息
        OutputTag receiptEventTag = new OutputTag<String>("receiptEventTag-side") {};

        // 读取订单数据
        KeyedStream<OrderEvents, String> orderStream = env.socketTextStream("localhost", 8888)./*readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/OrderEvents.csv").*/map(new MapFunction<String, OrderEvents>() {
            @Override
            public OrderEvents map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvents(Long.parseLong(split[0]), split[1], split[2], System.currentTimeMillis() / 1000);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvents>() {
            @Override
            public long extractAscendingTimestamp(OrderEvents element) {
                return element.getTimestamp() * 1000;
            }
        }).filter(new FilterFunction<OrderEvents>() {
            @Override
            public boolean filter(OrderEvents value) throws Exception {
                return value.getAction().equals("pay");
            }
        }).keyBy(new KeySelector<OrderEvents, String>() {
            @Override
            public String getKey(OrderEvents value) throws Exception {
                return value.getOrId();
            }
        });

        // 读取交易数据
        KeyedStream<ReceiptEvents, String> receiptStream = env.socketTextStream("localhost", 9999)./*readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/ReceiptLog.csv").*/map(new MapFunction<String, ReceiptEvents>() {
            @Override
            public ReceiptEvents map(String value) throws Exception {
                String[] split = value.split(",");
                return new ReceiptEvents(split[0], split[1], System.currentTimeMillis() / 1000);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvents>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvents element) {
                return element.getTimestamp() * 1000;
            }
        }).keyBy(new KeySelector<ReceiptEvents, String>() {
            @Override
            public String getKey(ReceiptEvents value) throws Exception {
                return value.getOrId();
            }
        });

        // connect两条流
        SingleOutputStreamOperator<String> process = orderStream.connect(receiptStream).process(new MyCoProcessFunction());

        // 输出正常交易的数据
        process.print("success");
        // 输出异常交易的数据
        process.getSideOutput(payEventTag).print("payEventTag");
        process.getSideOutput(receiptEventTag).print("receiptEventTag");

        env.execute("Tx Match job");
    }
}

class MyCoProcessFunction
        extends CoProcessFunction<OrderEvents, ReceiptEvents, String> {

    // 定义测输出流，输出只有pay事件没有receipt事件的异常信息
    OutputTag payEventTag = new OutputTag<String>("payEventTag-side") {};
    // 定义测输出流，输出只有receipt事件没有pay事件的异常信息
    OutputTag receiptEventTag = new OutputTag<String>("receiptEventTag-side") {};

    // 定义状态,保存订单pay事件和交易事件
    ValueState<OrderEvents> payEventValueState = null;
    ValueState<ReceiptEvents> receiptEventValueState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<OrderEvents> descriptor1
                = new ValueStateDescriptor<OrderEvents>("payEventValueState", OrderEvents.class);
        ValueStateDescriptor<ReceiptEvents> descriptor2
                = new ValueStateDescriptor<ReceiptEvents>("receiptEventValueState", ReceiptEvents.class);
        payEventValueState = getRuntimeContext().getState(descriptor1);
        receiptEventValueState = getRuntimeContext().getState(descriptor2);
    }

    // 处理OrderEvents事件
    @Override
    public void processElement1(OrderEvents orderEvents, Context ctx, Collector<String> out) throws Exception {
        if (receiptEventValueState.value() != null) {
            // 正常输出匹配
            out.collect("订单事件："+orderEvents.toString() + "和交易事件：" + receiptEventValueState.value().toString()+" 属于正常交易");
            receiptEventValueState.clear();
            payEventValueState.clear();
        } else {
            // 如果没有到账事件，注册定时器等待
            payEventValueState.update(orderEvents);
            ctx.timerService().registerEventTimeTimer(orderEvents.getTimestamp() * 1000 + 5000L); // 5s
        }
    }

    // 处理receipt事件
    @Override
    public void processElement2(ReceiptEvents receiptEvents, Context ctx, Collector<String> out) throws Exception {
        if (payEventValueState.value() != null) {
            // 正常输出
            out.collect("订单事件："+payEventValueState.value().toString() + "和交易事件：" + receiptEvents.toString()+" 属于正常交易");
            receiptEventValueState.clear();
            payEventValueState.clear();
        } else {
            // 如果没有订单事件，说明是乱序事件，注册定时器等待
            receiptEventValueState.update(receiptEvents);
            ctx.timerService().registerEventTimeTimer(receiptEvents.getTimestamp() * 1000 + 3000L); // 3s
        }
    }

    // 定时器
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 判断哪个状态存在，表示另一个事件没有来
        if (payEventValueState.value() != null) {
            ctx.output(payEventTag, payEventValueState.value().toString() + " 有pay事件没有receipt事件，属于异常事件");
        }

        if (receiptEventValueState.value() != null) {
            ctx.output(receiptEventTag, receiptEventValueState.value().toString() + " 有receipt事件没有pay事件。属于异常事件");
        }
        receiptEventValueState.clear();
        payEventValueState.clear();
    }
}

