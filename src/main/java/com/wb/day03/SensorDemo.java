package com.wb.day03;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 需求：
 * 从socket读取设备温度数据，如果每台设备连续三次温度大于40度，则报警
 */
public class SensorDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 事件时间
        //  env.getConfig().setAutoWatermarkInterval(200);// 周期性生成watermark，默认周期200ms
        fromSocket(env);
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static void fromSocket(StreamExecutionEnvironment env) throws Exception {
        OutputTag outputTag = new OutputTag<String>("out-side"){};
        SingleOutputStreamOperator<String> process = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Sensor>() {
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
        }).keyBy("deviceId").timeWindow(Time.seconds(5)).process(new MyProcessFunction(outputTag));
        process.print();
        process.getSideOutput(outputTag).print(); // 侧输出流

    }
}


class MyProcessFunction extends ProcessWindowFunction<Sensor, String, Tuple, TimeWindow> {
    public MyProcessFunction(OutputTag outputTag) {
        this.outputTag = outputTag;
    }

    public MyProcessFunction() {
    }

    ListState<Integer> listState;
    OutputTag outputTag;
    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor listStateDescriptor = new ListStateDescriptor("listState", Integer.class);
        listState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Sensor> elements, Collector<String> out) throws Exception {
        Iterator<Sensor> iterator = elements.iterator();
        List<Integer> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next().getTemperature());
        }
        int index = list.size() - 3;
        if (index >= 0) {
            for (int i = 0; i <= index; i++) {
                if (list.get(i) > 40 && list.get(i + 1) > 40 && list.get(1 + 2) > 40) {
                    out.collect("第" + i + "：" + list.get(i) + "、" + (i + 1) + "：" + list.get(i + 1) + "、" + (i + 2) + "：" + list.get(i + 2) + " 温度异常");
                    i += 2;
                }
            }
        }
        context.output(outputTag,"测输出流的数据-->"+context.window().getEnd()); // 侧输出流，可以用作分流

    }

    @Override
    public void clear(Context context) throws Exception {
        //super.clear(context);
        System.out.println("清空状态变量，"+context.window().getEnd());
        listState.clear();// 清空状态
    }
}


