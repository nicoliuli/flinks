package com.wb.day04.demo02;

import com.wb.common.Sensor;
import com.wb.common.Sensor1;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * 从kafka读取数据
 */
public class FlinkSqlDemo01SourceKafka {
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
                        .field("timestamps", DataTypes.TIMESTAMP(3)))
                .inAppendMode()
                .createTemporaryTable("inputTable");

        print(tabEnv);
        env.execute("Flink Streaming Java API Skeleton");
    }

    // upsert模式，向mysql写数据。
    private static void upsertSinkMysql(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,count(*) as cnt from inputTable group by deviceId";
        Table table = tabEnv.sqlQuery(sql);
        JDBCOptions options = JDBCOptions.builder()
                .setDBUrl("jdbc:mysql://xxx")
                .setDriverName("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("root")
                .setTableName("a_device_cnt") // mysql中的表，deviceId必须设置为primary key，否则达不到upsert,只能append
                .build();
        TableSchema schema = TableSchema.builder()
                .field("deviceId",DataTypes.STRING())
                .field("cnt",DataTypes.BIGINT())
                .build();
        JDBCUpsertTableSink sink = JDBCUpsertTableSink.builder()
                .setOptions(options)
                .setTableSchema(schema)
                .build();
        tabEnv.registerTableSink("output",sink);
        tabEnv.insertInto("output",table);
        /*
        CREATE TABLE `a_device_cnt` (
          `deviceId` varchar(64) NOT NULL,
          `cnt` int(11) DEFAULT NULL,
          PRIMARY KEY (`deviceId`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
         */

    }

    // 通过append模式，向mysql追加数据
    private static void appendSinkToMysql(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,temperature,timestamps from inputTable where deviceId like '%143%'";
        Table table = tabEnv.sqlQuery(sql);
        JDBCAppendTableSink tableSink = JDBCAppendTableSink.builder()
                .setDBUrl("jdbc:mysql://root")
                .setUsername("root")
                .setPassword("root")
                .setDrivername("com.mysql.jdbc.Driver")
                .setQuery("insert into a_device(deviceId,temperature,timestamps) values(?,?,?)")
                .setParameterTypes(TypeInformation.of(String.class), TypeInformation.of(Integer.class), TypeInformation.of(Long.class))
                .build();
        TypeInformation[] fieldTypes = {Types.STRING, Types.INT,Types.LONG};
        tabEnv.registerTableSink("jdbcSink",new String[]{"deviceId","temperature","timestamps"},fieldTypes,tableSink);
        tabEnv.insertInto("jdbcSink",table);
        /*
        CREATE TABLE `a_device_cnt` (
          `deviceId` varchar(64) NOT NULL,
          `cnt` int(11) DEFAULT NULL,
          PRIMARY KEY (`deviceId`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
         */
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

    //这么写报错，但不知道为什么
    private static void group(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,count(deviceId) as nums from inputTable group by deviceId";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toRetractStream(table, Count.class).print();
    }

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

    private static void print(StreamTableEnvironment tabEnv){
        String sql = "select deviceId,temperature,timestamps from inputTable";
        Table table = tabEnv.sqlQuery(sql);
        tabEnv.toAppendStream(table, Sensor.class).print();
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
