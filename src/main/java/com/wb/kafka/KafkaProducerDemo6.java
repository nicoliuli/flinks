package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.TestModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo6 {

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
                TestModel t = new TestModel();
              //  t.setT_id(new Random().nextInt(20));
                producer.send(new ProducerRecord<String, String>("test_join", JSON.toJSONString(t)));
                Thread.sleep(500);
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }




}