package com.wb.day04.demo01;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 *
 */
public class TableApiDemo1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Sensor> dataStream = env.readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/aaa.txt").map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new Sensor(split[0], Integer.parseInt(split[1]), System.currentTimeMillis());
            }
        });

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table table = tableEnv.fromDataStream(dataStream);

        // 调用table api进行转换
        Table resultTab = table.select("deviceId,temperature,timestamps").where("deviceId='device1'");

        // 将Table转化为DataStream输出
        tableEnv.toAppendStream(resultTab, Sensor.class).print();

        env.execute("Flink Streaming Java API Skeleton");
    }


}
