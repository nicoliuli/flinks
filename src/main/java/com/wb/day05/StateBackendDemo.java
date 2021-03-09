package com.wb.day05;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StateBackendDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // checkpoint时间
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flinkcp/demo1"));

        // 配置重启策略,默认无限重启
     //   env.setRestartStrategy(RestartStrategies.noRestart()); 不重启

        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static void fromSocket(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                if(value.equals("bug")){
                    throw new Exception("bug");
                }
                return new Sensor("deviceId1", Integer.parseInt(value), System.currentTimeMillis());
            }
        }).keyBy("deviceId").process(new MyKeyedProcessFunction()).print();
    }
}


class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Sensor, String> {

    ValueState<Integer> valueStat = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        /*设置状态过期时间
        StateTtlConfig cfg = StateTtlConfig
                .newBuilder(Time.seconds(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 创建时和写时更新，OnReadAndWrite读写都会更新
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 过期后不可用  ReturnExpiredIfNotCleanedUp未清理则可用
                .build();*/
        ValueStateDescriptor<Integer> itemViewStateDesc = new ValueStateDescriptor<Integer>(
                "valuestat", Integer.class
        );
        //itemViewStateDesc.enableTimeToLive(cfg);
        valueStat = getRuntimeContext().getState(itemViewStateDesc);
    }

    @Override
    public void processElement(Sensor value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(value.toString());
        if (valueStat.value() == null) {
            valueStat.update(0);
        }
        valueStat.update(valueStat.value() + value.getTemperature());

        long timer = System.currentTimeMillis() + 1000;
        ctx.timerService().registerProcessingTimeTimer(timer);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("定时器输出状态值：" + valueStat.value());
    }
}