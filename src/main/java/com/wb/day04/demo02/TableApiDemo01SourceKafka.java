package com.wb.day04.demo02;

import com.wb.common.Sensor1;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从kafka读取数据
 */
public class TableApiDemo01SourceKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Json())
                .withSchema(new Schema()    // 定义表结构，并指定字段
                        .field("deviceId", DataTypes.STRING())
                        .field("temperature", DataTypes.INT())
                        .field("timestamps", DataTypes.BIGINT()))
                .createTemporaryTable("inputTable");

        sinkToKafka(tabEnv);
        env.execute("Flink Streaming Java API Skeleton");
    }



    // 从kafka读取数据，经过sql过滤出想要的数据，再写入到kafka
    private static void sinkToKafka(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,temperature,timestamps from inputTable where deviceId like '%143%'";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.connect(new Kafka().version("0.11")
                .topic("sinkkafka")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Json())
                .withSchema(new Schema().field("deviceId",DataTypes.STRING())
                        .field("temperature",DataTypes.INT())
                        .field("timestamps",DataTypes.BIGINT()))
                .createTemporaryTable("output");
        tabEnv.insertInto("output",table);
    }

    /*这么写报错，但不知道为什么
    private static void group(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,count(*) as nums from inputTable group by deviceId";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toRetractStream(table, Count.class).print();
    }*/

    // like and语句
    private static void likeAnd(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,temperature as tp,timestamps from inputTable where deviceId like '%143%' and temperature>40";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Sensor1.class).print();
    }

    // like 语句
    private static void like(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,temperature as tp,timestamps from inputTable where deviceId like '%1435%'";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Sensor1.class).print();
    }
}

class Count{
    public String deviceId;

    public Long nums;

    public Count() {
    }

    public Count(String deviceId, Long nums) {
        this.deviceId = deviceId;
        this.nums = nums;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getNums() {
        return nums;
    }

    public void setNums(Long nums) {
        this.nums = nums;
    }
}
