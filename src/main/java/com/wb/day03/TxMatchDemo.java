package com.wb.day03;

import com.wb.common.OrderEvent;
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
        fromCsv(env);

        env.execute("order timeout job");
    }

    private static void fromCsv(StreamExecutionEnvironment env) throws Exception {

        OutputTag orderEventTag = new OutputTag<String>("orderEventTag-side") {};
        OutputTag receiptEventTag = new OutputTag<String>("receiptEventTag-side") {};

        // 读取订单数据
        KeyedStream<OrderEvents, String> stream1 = env.readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/OrderEvents.csv").map(new MapFunction<String, OrderEvents>() {
            @Override
            public OrderEvents map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvents(Long.parseLong(split[0]), split[1], split[2],Long.parseLong(split[3]));
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
        KeyedStream<ReceiptEvents, String> stream2 = env.readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/ReceiptLog.csv").map(new MapFunction<String, ReceiptEvents>() {
            @Override
            public ReceiptEvents map(String value) throws Exception {
                String[] split = value.split(",");
                return new ReceiptEvents(split[0], split[1], Long.parseLong(split[2]));
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

        SingleOutputStreamOperator<String> process = stream1.connect(stream2).process(new MyCoProcessFunction());

        process.print("process");
        process.getSideOutput(orderEventTag).print("orderEventTag");
        process.getSideOutput(receiptEventTag).print("receiptEventTag");
    }


}

class MyCoProcessFunction extends CoProcessFunction<OrderEvents, ReceiptEvents, String> {

    OutputTag orderEventTag = new OutputTag<String>("orderEventTag-side") {};
    OutputTag receiptEventTag = new OutputTag<String>("receiptEventTag-side") {};

    ValueState<OrderEvents> orderEventValueState = null;
    ValueState<ReceiptEvents> receiptEventValueState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<OrderEvents> descriptor1 = new ValueStateDescriptor<OrderEvents>("orderEventValueState", OrderEvents.class);
        ValueStateDescriptor<ReceiptEvents> descriptor2 = new ValueStateDescriptor<ReceiptEvents>("receiptEventValueState", ReceiptEvents.class);
        orderEventValueState = getRuntimeContext().getState(descriptor1);
        receiptEventValueState = getRuntimeContext().getState(descriptor2);
    }

    @Override
    public void processElement1(OrderEvents value, Context ctx, Collector<String> out) throws Exception {
        if (receiptEventValueState.value() != null) {
            // 正常输出匹配
            out.collect(value.toString() + "<-->" + receiptEventValueState.value().toString());
            receiptEventValueState.clear();
            orderEventValueState.clear();
        } else {
            // 如果没有到账事件，注册定时器等待
            orderEventValueState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000+5000L); // 3s
        }
    }


    @Override
    public void processElement2(ReceiptEvents value, Context ctx, Collector<String> out) throws Exception {
        if (orderEventValueState.value() != null) {
            // 正常输出
            out.collect(orderEventValueState.value().toString() + "<-->" + value.toString());
            receiptEventValueState.clear();
            orderEventValueState.clear();
        } else {
            receiptEventValueState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000+3000L); // 3s
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
      //  System.out.println("timer==>"+timestamp);
        // 判断哪个状态存在，表示另一个没来
        if (orderEventValueState.value() != null) {
            ctx.output(orderEventTag, orderEventValueState.value().toString() + " 有pay事件没有receipt事件");
        }

        if (receiptEventValueState.value() != null) {
            ctx.output(receiptEventTag, receiptEventValueState.value().toString() + " 有receipt事件没有pay事件");
        }
        receiptEventValueState.clear();
        orderEventValueState.clear();
    }

}

class OrderEvents{
    private Long userId;
    private String action;
    private String orId;
    private Long timestamp;

    public OrderEvents() {
    }

    public OrderEvents(Long userId, String action, String orId, Long timestamp) {
        this.userId = userId;
        this.action = action;
        this.orId = orId;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getOrId() {
        return orId;
    }

    public void setOrId(String orId) {
        this.orId = orId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvents{" +
                "userId=" + userId +
                ", action='" + action + '\'' +
                ", orId='" + orId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

class ReceiptEvents {
    private String orId;
    private String payEquipment;
    private Long timestamp;

    public String getOrId() {
        return orId;
    }

    public void setOrId(String orId) {
        this.orId = orId;
    }

    public String getPayEquipment() {
        return payEquipment;
    }

    public void setPayEquipment(String payEquipment) {
        this.payEquipment = payEquipment;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ReceiptEvents() {
    }

    public ReceiptEvents(String orId, String payEquipment, Long timestamp) {
        this.orId = orId;
        this.payEquipment = payEquipment;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ReceiptEvents{" +
                "orId='" + orId + '\'' +
                ", payEquipment='" + payEquipment + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}