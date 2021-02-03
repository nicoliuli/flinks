package com.wb.day04.demo02;

import com.alibaba.fastjson.JSON;
import com.wb.common.Log;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 每个uid请求的次数
 * count，groupby
 * 写入mysql
 */
public class GroupByDemo1 {
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
        String sql = "SELECT uid,count(*) from log group by uid";
        Table table = tableEnv.sqlQuery(sql);
        JDBCOptions options = JDBCOptions.builder()
                .setDBUrl("jdbc:mysql://xxxx")
                .setDriverName("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("xxxx")
                .setTableName("a_user_cnt") // mysql中的表，deviceId必须设置为primary key，否则达不到upsert,只能append
                .build();
        TableSchema schema = TableSchema.builder()
                .field("uid", DataTypes.STRING())
                .field("cnt",DataTypes.BIGINT())
                .build();
        JDBCUpsertTableSink sink = JDBCUpsertTableSink.builder()
                .setOptions(options)
                .setTableSchema(schema)
                .build();
        tableEnv.registerTableSink("user_cnt",sink);
        tableEnv.insertInto("user_cnt",table);


        env.execute("Flink Streaming Java API Skeleton");
    }
/*

 CREATE TABLE `a_user_cnt` (
 `uid` varchar(64) NOT NULL,
 `cnt` int(11) DEFAULT NULL,
 PRIMARY KEY (`uid`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

 */

}

