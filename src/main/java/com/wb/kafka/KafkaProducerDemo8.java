package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.UserBehavior1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * flink1.12.0
 */
public class KafkaProducerDemo8 {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;



        try {
            producer = new KafkaProducer<String, String>(properties);
            while (true){
                UserBehavior1 userBehavior = new UserBehavior1();
                userBehavior.setUser_id(Long.parseLong(new Random().nextInt(200)+""));
                userBehavior.setCategory_id(Long.parseLong(new Random().nextInt(50)+""));
                userBehavior.setTs(System.currentTimeMillis());
                userBehavior.setBehavior(new Random().nextBoolean()?"buy":"order");
                userBehavior.setItem_id(Long.parseLong(new Random().nextInt(200)+""));
                producer.send(new ProducerRecord<String, String>("user_behavior4", JSON.toJSONString(userBehavior)));
                Thread.sleep(5);
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }


/*

select s.shopname,od.shopid,count(*) as cnt from order_table od left join shop_table s on od.shopid=s.id  group by od.shopid,s.shopname;



 */

}