package com.wb.day04.demo02;

import com.wb.common.Sensor;
import com.wb.common.Sensor2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class FlinkSqlDemo02Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Sensor> socketStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                Sensor s = new Sensor(value, 1, System.currentTimeMillis());
                return s;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Sensor element) {
                return element.getTimestamps();
            }
        });
        eventTime(tabEnv,socketStream);
        env.execute("Flink Streaming Java API Skeleton");
    }


    // 指定事件时间
    private static void eventTime(StreamTableEnvironment tabEnv,SingleOutputStreamOperator<Sensor> socketStream) {
        // 流转化表，并指定事件时间，注意：事件时间必须用Timestamp类型
        Table table = tabEnv.fromDataStream(socketStream,"deviceId,temperature,timestamps,eventTime.rowtime");
        //    table.printSchema();
        tabEnv.toAppendStream(table, Sensor2.class).print();
    }



}


