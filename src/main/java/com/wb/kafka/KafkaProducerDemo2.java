package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.Sensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaProducerDemo2 {
    public static void main(String[] args) {
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
            while (true) {
                Sensor model = new Sensor();
                model.setDeviceId(UUID.randomUUID().toString());
                model.setTimestamps(System.currentTimeMillis());
                model.setTemperature(new Random().nextInt(50));
                producer.send(new ProducerRecord<String, String>("test", JSON.toJSONString(model)));

            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
