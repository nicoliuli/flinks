package com.wb.day03;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAllDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 事件时间
        //  env.getConfig().setAutoWatermarkInterval(200);// 周期性生成watermark，默认周期200ms
        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }


    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                // String[] split = value.split(" ");
                return new Sensor("device1", Integer.parseInt(value), System.currentTimeMillis());
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.milliseconds(30)) {
            @Override
            public long extractTimestamp(Sensor element) {
                return element.getTimestamp();
            }
        }).timeWindowAll(Time.seconds(5)).reduce(new ReduceFunction<Sensor>() {
            @Override
            public Sensor reduce(Sensor value1, Sensor value2) throws Exception {
                return new Sensor(value1.getDeviceId(),value1.getTemperature()+value2.getTemperature(),System.currentTimeMillis());
            }
        }).print();
    }
}

class MyProcessFunctions extends ProcessAllWindowFunction<Sensor, String, TimeWindow> {
    ListState<Integer> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor listStateDescriptor = new ListStateDescriptor("listState", Integer.class);
        listState = getRuntimeContext().getListState(listStateDescriptor);
    }


    @Override
    public void process(Context context, Iterable<Sensor> elements, Collector<String> out) throws Exception {

    }
}

