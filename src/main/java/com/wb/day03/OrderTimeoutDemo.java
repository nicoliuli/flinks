package com.wb.day03;

import com.wb.common.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 订单超时检测

/*
输入；
34729,create,1558430842
34730,create,1558430843
34729,pay,1558430844
34730,modify,1558430845
34731,create,1558430846
34731,pay,1558430849
34732,create,1558430852
34733,create,1558430855
34734,create,1558430859
34734,create,1558431000
34733,pay,1558431000
34732,pay,1558449999
 */
public class OrderTimeoutDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fromSocket(env);

        env.execute("order timeout job");
    }

    private static void fromSocket(StreamExecutionEnvironment env) {
        OutputTag outputTag = new OutputTag<String>("out-side") {
        };
        SingleOutputStreamOperator<String> dataStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTime() * 1000;
            }
        }).keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.getOrderId();
            }
        }).process(new KeyedProcessFunction<Long, OrderEvent, String>() {
            ValueState<Boolean> createState = null;
            ValueState<Boolean> payState = null;
            ValueState<Long> timeState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Boolean> createDesc = new ValueStateDescriptor<Boolean>("createState", Boolean.class);
                createState = getRuntimeContext().getState(createDesc);
                ValueStateDescriptor<Boolean> payDesc = new ValueStateDescriptor<Boolean>("payState", Boolean.class);
                payState = getRuntimeContext().getState(payDesc);
                ValueStateDescriptor<Long> timeDesc = new ValueStateDescriptor<Long>("timeState", Long.class);
                timeState = getRuntimeContext().getState(timeDesc);
            }

            @Override
            public void processElement(OrderEvent event, Context ctx, Collector<String> out) throws Exception {
                Boolean isPay = payState.value();
                Long timerTs = timeState.value();
                if (isPay == null) isPay = false;
                if (timerTs == null) timerTs = 0L;

                if (event.getType().equals("create")) {
                    if (isPay == true) { // 乱序事件，先pay后create
                        out.collect(event + " 属于正常订单1");
                        ctx.timerService().deleteEventTimeTimer(timerTs);
                        payState.clear();
                        timeState.clear();
                    } else {
                        long time = event.getTime() * 1000 + 600 * 1000;
                        ctx.timerService().registerEventTimeTimer(time);
                        timeState.update(time);
                    }
                } else if (event.getType().equals("pay")) {
                    if (timerTs > 0) {
                        if (timerTs > event.getTime() * 1000) {
                            out.collect(event + " 属于正常订单2");
                        } else {
                            ctx.output(outputTag, event + " time out");
                        }
                    } else { // pay事件先来，注册定时器等create事件
                        payState.update(true);
                        ctx.timerService().registerEventTimeTimer(event.getTime() * 1000L);
                        timeState.update(event.getTime() * 1000L);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                Boolean isPay = payState.value();
                if (isPay == null) isPay = false;
                if (isPay == true) {
                    ctx.output(outputTag, ctx.getCurrentKey() + " payed,but not create");
                } else {
                    ctx.output(outputTag, ctx.getCurrentKey() + " order time out");
                }
                payState.clear();
                timeState.clear();
            }
        });

        dataStream.print("payed");
        dataStream.getSideOutput(outputTag).print("timer out order");
    }
}
/*

                                  _oo0oo_
                                 088888880
                                 88" . "88
                                 (| -_- |)
                                  0\ = /0
                               ___/'---'\___
                             .' \\|     |// '.
                            / \\|||  :  |||// \
                           /_ ||||| -:- |||||- \
                          |   | \\\  -  /// |   |
                          | \_|  ''\---/''  |_/ |
                          \  .-\__  '-'  __/-.  /
                        ___'. .'  /--.--\  '. .'___
                     ."" '<  '.___\_<|>_/___.' >'  "".
                    | | : '-  \'.;'\ _ /';.'/ - ' : | |
                    \  \ '_.   \_ __\ /__ _/   .-' /  /
                ====='-.____'.___ \_____/___.-'____.-'=====
                                  '=---='


              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                        佛祖保佑    iii    永不宕机
 */