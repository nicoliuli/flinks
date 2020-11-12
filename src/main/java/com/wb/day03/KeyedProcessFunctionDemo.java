package com.wb.day03;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  env.getConfig().setAutoWatermarkInterval(200);// 周期性生成watermark，默认周期200ms
        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static void fromSocket(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                return new Sensor(System.currentTimeMillis() % 2 == 0 ? "deviceId1" : "deviceId2", Integer.parseInt(value), System.currentTimeMillis());
            }
        }).keyBy("deviceId").process(new MyKeyedProcessFunction()).print();
    }
}

class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Sensor, String> {

    ListState<Sensor> list = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Sensor> itemViewStateDesc = new ListStateDescriptor<Sensor>(
                "liststat", Sensor.class
        );
        list = getRuntimeContext().getListState(itemViewStateDesc);
    }

    @Override
    public void processElement(Sensor value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(value.toString());
        list.add(value);
        long timer = System.currentTimeMillis()+1000;
        System.out.println("注册定时器："+timer);
        ctx.timerService().registerProcessingTimeTimer(timer);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("触发定时器："+timestamp);
    }
}
