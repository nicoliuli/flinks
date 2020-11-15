package com.wb.day04;

import com.wb.common.Sensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从kafka读取数据
 * 输入：String,Integer,Long
 * device1,2,333
 */
public class TableApiDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        String sql = "select deviceId,temperature,timestamps from inputTable where deviceId='device1'";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Sensor.class).print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}
