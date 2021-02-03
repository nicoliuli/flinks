package com.wb.day01;


import com.alibaba.fastjson.JSON;
import com.wb.common.Word;
import com.wb.common.WordInput;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 文件倒排索引
 * 数据源：com.wb.kafka.KafkaProducerDemo3
 */
public class IndexDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Word> dataStream = env
                .addSource(new FlinkKafkaConsumer011<String>("word", new SimpleStringSchema(), kafkaProp()))
                .flatMap(new FlatMapFunction<String, Word>() {
                    @Override
                    public void flatMap(String value, Collector<Word> out) throws Exception {
                        // 对原始数据做ETL转化
                        WordInput input = JSON.parseObject(value, WordInput.class);
                        String line = input.getLine();
                        String fileName = input.getFileName();
                        line = line.replace(",", " ")
                                .replace(".", " ")
                                .replace(";", " ")
                                .replace(":", " ")
                                .replace("\"", " ")
                                .replace(")", " ")
                                .replace("(", " ")
                                .replace("{", " ")
                                .replace("}", " ");

                        String[] words = line.split(" ");
                        for (String aWord : words) {
                            if (aWord.length() < 5) {
                                continue;
                            }
                            Long id = Long.parseLong((aWord + fileName).hashCode() + "");
                            if (id < 0) {
                                id = -id;
                            }
                            Word word = new Word(id, aWord, fileName);
                            out.collect(word);
                        }
                    }
                });

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        tabEnv.createTemporaryView("wordSource", dataStream);
        // 构建倒排索引并写入到下游存储
        buildIndexAndSink(tabEnv);
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static void buildIndexAndSink(StreamTableEnvironment tabEnv) {
        String sql = "select id,word,filePath,count(*) as cnt from wordSource group by id,word,filePath";
        Table table = tabEnv.sqlQuery(sql);

        JDBCOptions options = JDBCOptions.builder()
                .setDBUrl("jdbc:mysql://xxxx")
                .setDriverName("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("root")
                .setTableName("a_word_cnt")
                .build();
        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.BIGINT())
                .field("word", DataTypes.STRING())
                .field("filePath", DataTypes.STRING())
                .field("cnt", DataTypes.BIGINT())
                .build();
        JDBCUpsertTableSink sink = JDBCUpsertTableSink.builder()
                .setOptions(options)
                .setTableSchema(schema)
                .build();
        tabEnv.registerTableSink("outputSink", sink);

        tabEnv.insertInto("outputSink", table);
    }

    private static Properties kafkaProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("zookeeper.connect", "localhost:2181");
        prop.put("group.id", "word");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("auto.offset.reset", "latest");
        return prop;
    }

}



