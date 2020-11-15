package com.wb.day04;

import com.wb.common.Sensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从文件系统读取数据
 */
public class TableApiDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.connect(new FileSystem().path("/Users/liuli/code/flink/flinks/src/main/resources/aaa.txt")) //定义文件系统的连接
        .withFormat(/*new OldCsv()*/new Csv()) // 定义以csv的格式进行格式化
        .withSchema(new Schema()    // 定义表结构，并指定字段
        .field("deviceId", DataTypes.STRING())
        .field("temperature",DataTypes.INT())
        .field("timestamps",DataTypes.BIGINT()))
        .createTemporaryTable("inputTable"); //创建临时表

        Table table = tabEnv.from("inputTable");

        tabEnv.toAppendStream(table, Sensor.class).print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
