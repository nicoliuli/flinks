package com.wb.day04.demo01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从kafka读取数据
 * csv输入：String,Integer,Long
 * device1,2,333
 * json输入：{"deviceId":"device1","temperature":1,"timestamps":2}
 *
 * 输出到kafka
 */
public class TableApiOutput2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        /*
        json输入-->json输出
        csv输入-->csv输出
        json输入-->csv输出
        csv输入-->json输出
         */

        tabEnv.connect(new Kafka()
                .version("0.11").topic("sensor")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","localhost:9092"))
                .withFormat(new Json())
                //.withFormat(new Csv())
                .withSchema(new Schema()    // 定义表结构，并指定字段
                        .field("deviceId", DataTypes.STRING())
                        .field("temperature",DataTypes.INT())
                        .field("timestamps",DataTypes.BIGINT()))
                .createTemporaryTable("inputTable");

        Table table = tabEnv.from("inputTable");
        Table resultTable = table.select("deviceId,temperature,timestamps").where("deviceId='device1'");

        // 定义输出kafka
        tabEnv.connect(new Kafka()
            .version("0.11").topic("sensor_output")
            .property("zookeeper.connect","localhost:2181")
            .property("bootstrap.servers","localhost:9092"))
                //.withFormat(new Json())
                .withFormat(new Csv())
                .withSchema(new Schema().field("deviceId", DataTypes.STRING())
                        .field("temperature",DataTypes.INT())
                        .field("timestamps",DataTypes.BIGINT()))
                .createTemporaryTable("output");

        tabEnv.insertInto("output",resultTable);

        env.execute("Flink Streaming Java API Skeleton");
    }
}
