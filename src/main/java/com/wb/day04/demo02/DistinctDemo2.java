package com.wb.day04.demo02;

import com.alibaba.fastjson.JSON;
import com.wb.common.Log;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 对日志进行ETL，取出uid统计uid个数(去重后)
 */
public class DistinctDemo2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        SingleOutputStreamOperator<Log> dataStream = env.readTextFile("/Users/liuli/Downloads/downloaded_data.txt").map(new MapFunction<String, Log>() {
            @Override
            public Log map(String value) throws Exception {
                String log = JSON.parseObject(value).getString("content");
                return JSON.parseObject(log.split("requestParam :")[1].split(", response :")[0],Log.class);
            }
        });

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);





        tableEnv.createTemporaryView("log",dataStream,"uid");
        String sql = "SELECT count( distinct uid) FROM log";
        Table tableSql = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(tableSql,Long.class).print();


        env.execute("Flink Streaming Java API Skeleton");
    }


}
