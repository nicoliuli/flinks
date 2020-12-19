package com.wb.day04.demo01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * 从kafka读取数据
 * 输入：String,Integer,Long
 * device1,2,333
 * 输出到文件
 */
public class TableApiOutput1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.connect(new Kafka()
                .version("0.11").topic("sensor")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()    // 定义表结构，并指定字段
                        .field("deviceId", DataTypes.STRING())
                        .field("temperature",DataTypes.INT())
                        .field("timestamps",DataTypes.BIGINT()))
                .createTemporaryTable("inputTable");

        Table table = tabEnv.from("inputTable");
        Table resultTable = table.select("deviceId,temperature,timestamps").where("deviceId='device1'");

        // 定义输出文件
        tabEnv.connect(new FileSystem().path("/Users/liuli/code/flink/flinks/src/main/resources/out.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("deviceId",DataTypes.STRING())
                        .field("temperature",DataTypes.INT())
                        .field("timestamps",DataTypes.BIGINT()))
                .createTemporaryTable("output");

        tabEnv.insertInto("output",resultTable);

        env.execute("Flink Streaming Java API Skeleton");
    }
}
