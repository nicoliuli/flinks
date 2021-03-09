package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.UserBehavior;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerDemo5 {

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
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            while (true){
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setUser_id(Long.parseLong(new Random().nextInt(200)+""));
                userBehavior.setCategory_id(Long.parseLong(new Random().nextInt(50)+""));
                userBehavior.setTs(simpleDateFormat.format(new Date()));
                userBehavior.setBehavior(new Random().nextBoolean()?"buy":"order");
                userBehavior.setItem_id(Long.parseLong(new Random().nextInt(200)+""));
                producer.send(new ProducerRecord<String, String>("user_behavior4", JSON.toJSONString(userBehavior)));
                Thread.sleep(10);
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }




}