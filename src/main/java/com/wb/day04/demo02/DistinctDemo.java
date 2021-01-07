package com.wb.day04.demo02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 统计一个文件里uid有多少个（去重后）
 */
public class DistinctDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        SingleOutputStreamOperator<String> dataStream = env.readTextFile("/Users/liuli/code/flink/flinks/src/main/resources/aaaa.txt").map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table table = tableEnv.fromDataStream(dataStream);



        tableEnv.createTemporaryView("uids",dataStream,"uid");
        String sql = "SELECT COUNT(DISTINCT uid) FROM uids";
        Table tableSql = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(tableSql,Long.class).print();


        env.execute("Flink Streaming Java API Skeleton");
    }


}
