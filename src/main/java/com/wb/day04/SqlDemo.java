package com.wb.day04;

import com.wb.common.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class SqlDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Sensor> dataStream = env.readTextFile("/Users/liuli/aaa.txt").map(new MapFunction<String, Sensor>() {
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

    //    tableEnv.createTemporaryView("sensor",table); 基于Table创建一张视图,等价于下一行

        tableEnv.createTemporaryView("sensor",dataStream); // 基于DataStream创建一张视图，和上一行等价
        String sql = "select deviceId,temperature,timestamps from sensor where deviceId='device1'";
        Table tableSql = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(tableSql,Sensor.class).print();


        env.execute("Flink Streaming Java API Skeleton");
    }


}
