package com.wb.day01;

import com.alibaba.fastjson.JSON;
import com.wb.common.risk.Pay;
import com.wb.common.risk.Rule;
import com.wb.common.risk.Wrapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class Risk {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        logic(env);
        env.execute("job");
    }

    private static void logic(StreamExecutionEnvironment env) {
        MapStateDescriptor<Integer, Rule> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", Integer.class, Rule.class);


        // 规则广播流
        BroadcastStream<Rule> ruleStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Rule>() {
            @Override
            public Rule map(String value) throws Exception {
                String[] array = value.split(" ");
                // ruleId,groupKeyName,limit,window,aggregateFunctionType
                Rule rule = new Rule(Integer.parseInt(array[0]), array[1], Integer.parseInt(array[2]), Long.parseLong(array[3]), array[4]);
                return rule;
            }
        }).broadcast(ruleStateDescriptor);

        // 业务数据流,启动com.wb.kafka.KafkaProducerDemo4
        SingleOutputStreamOperator<Pay> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("risk", new SimpleStringSchema(), kafkaProp())).map(new MapFunction<String, Pay>() {
            @Override
            public Pay map(String value) throws Exception {
                return JSON.parseObject(value, Pay.class);
            }
        });

        dataStream.connect(ruleStream).process(new BroadcastProcessFunction<Pay, Rule, Wrapper>() {
            MapStateDescriptor<Integer, Rule> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", Integer.class, Rule.class);

            @Override
            public void processElement(Pay pay, ReadOnlyContext ctx, Collector<Wrapper> out) throws Exception {
                // 包装成wrapper
                Iterable<Map.Entry<Integer, Rule>> ruleEntry = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();
                Integer ruleId = pay.getRuleId();
                for (Map.Entry<Integer, Rule> rule : ruleEntry) {
                    if (ruleId.equals(rule.getValue().getRuleId())) {
                        Wrapper wrapper = new Wrapper(getGroupKey(pay, rule.getValue().getGroupKeyName()), pay);
                        out.collect(wrapper);
                    }
                }
            }

            @Override
            public void processBroadcastElement(Rule rule, Context ctx, Collector<Wrapper> out) throws Exception {
                BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                broadcastState.put(rule.getRuleId(), rule);

            }
        }).keyBy(new KeySelector<Wrapper, String>() {
            @Override
            public String getKey(Wrapper wrapper) throws Exception {
                return wrapper.getKey();
            }
        }).connect(ruleStream).process(new KeyedBroadcastProcessFunction<String, Wrapper, Rule, Object>() {
            // 存储广播状态
            MapStateDescriptor<Integer, Rule> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState", Integer.class, Rule.class);
            // 存储风控业务数据
            MapState<String, List<Integer>> riskMap = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor mapDescriptor = new MapStateDescriptor("", String.class, List.class);
                riskMap = getRuntimeContext().getMapState(mapDescriptor);
            }

            @Override
            public void processElement(Wrapper wrapper, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                String groupKeyName = wrapper.getKey();
                Pay pay = wrapper.getPay();
                Rule rule = null;
                Iterable<Map.Entry<Integer, Rule>> ruleEntries = ctx.getBroadcastState(ruleStateDescriptor).immutableEntries();
                for (Map.Entry<Integer, Rule> ruleEntry : ruleEntries) {
                    if (ruleEntry.getKey().equals(pay.getRuleId())) {
                        rule = ruleEntry.getValue();
                    }
                }

                //  写到状态
                String groupKey = groupKeyName + "_" + getWindowWidth(pay.getEventTime(),rule.getWindow());
                putStatus(riskMap,groupKey,pay.getAmount());

                // 注册定时器

                // 风控校验

                // alert
            }

            @Override
            public void processBroadcastElement(Rule rule, Context ctx, Collector<Object> out) throws Exception {
                BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                broadcastState.put(rule.getRuleId(), rule);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                // 清空过期状态
            }
        });
    }


    private static Properties kafkaProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("zookeeper.connect", "localhost:2181");
        prop.put("group.id", "risk");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("auto.offset.reset", "latest");
        return prop;
    }

    private static String getGroupKey(Pay pay, String groupKeyName) {
        String[] groupKeyNames = groupKeyName.split("_");
        String key = "";
        for (String keyName : groupKeyNames) {
            if ("fromUid".equals(keyName)) {
                key = key + pay.getFromUid();
            }
            if ("toUid".equals(keyName)) {
                key = key + pay.getToUid();
            }
            key = key + "_";
        }
        return key;
    }

    private static String getWindowWidth(long eventTime,long window){
        String pattern = "yyyy-MM-dd:HH:mm";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        String windowEnd = sdf.format(new Date(eventTime));
        String windowStart = sdf.format(new Date(eventTime - window * 1000));
        return windowStart+"_"+windowEnd;
    }

    private static void putStatus(MapState<String, List<Integer>> riskMap, String groupKey, Integer value) {
        List<Integer> list = null;
        try {
            list = riskMap.get(groupKey);
            if (list == null || list.isEmpty()) {
                list = new ArrayList<>();
                list.add(value);
            } else {
                list.add(value);
            }
            riskMap.put(groupKey, list);
            for (Map.Entry<String, List<Integer>> entry : riskMap.entries()) {
                System.out.println("key = " + entry.getKey() + ",value = " + entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}



