package com.wb.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkRedisDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        redisSink(env);
        env.execute("Flink Streaming Java API Skeleton");

    }

    private static void redisSink(StreamExecutionEnvironment env) {
        // 定义一个redis的config，FlinkJedisConfigBase是基类，它的子类支持单机、集群等配置
        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379)
                .build();

        env.socketTextStream("localhost", 8888).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).addSink(new RedisSink<>(flinkJedisConfigBase, new MyRedisHashMapper()));
    }
}

// 写入到redis list里
class MyRedisListMapper implements RedisMapper {

    @Override
    public RedisCommandDescription getCommandDescription() {


        return new RedisCommandDescription(RedisCommand.LPUSH); // 一个参数的构造方法，用在key-value结构中
    }

    @Override
    public String getKeyFromData(Object o) { // 标识 key
        return "flink-list";
    }

    @Override
    public String getValueFromData(Object o) { // 标识 value
        return o.toString();
    }
}

// 写到redis hash里
class MyRedisHashMapper implements RedisMapper {

    @Override
    public RedisCommandDescription getCommandDescription() {
        // 两个参数的构造方法，主要是用在对 hash、zset的操作中
        // return new RedisCommandDescription(RedisCommand.HSET,"key");
        // return new RedisCommandDescription(RedisCommand.ZADD,"key");
        return new RedisCommandDescription(RedisCommand.HSET, "flink-hash");
    }

    @Override
    public String getKeyFromData(Object data) { // 标识filed
        return data.toString();
    }

    @Override
    public String getValueFromData(Object data) { //标识value
        return data.toString();
    }
}



/*  官方示例
private static class RedisTestMapper implements RedisMapper<Tuple2<String, String>> {
 *    public RedisCommandDescription getCommandDescription() {
 *        return new RedisCommandDescription(RedisCommand.PUBLISH);
 *    }
 *    public String getKeyFromData(Tuple2<String, String> data) {
 *        return data.f0;
 *    }
 *    public String getValueFromData(Tuple2<String, String> data) {
 *        return data.f1;
 *    }
 *}

 */
