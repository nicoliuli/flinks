package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.risk.Pay;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerDemo4 {

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


        String [] fromUids = {"123","234","345","456","567","678","789"};
        String [] toUids = {"987","876","765","654","543","432","321"};
        Integer [] amounts = {1,2,3,4,5,6,8,9,10,20,30,40,50,60,70,80,90,100};
        Integer [] ruleIds = {1,2};
        try {
            producer = new KafkaProducer<String, String>(properties);

            while (true){
           //     Thread.sleep(100);
                String fromUid = fromUids[new Random().nextInt(fromUids.length)];
                String toUid = toUids[new Random().nextInt(toUids.length)];
                Integer amount = amounts[new Random().nextInt(amounts.length)];
                Integer ruleId = ruleIds[new Random().nextInt(ruleIds.length)];
                Pay pay = new Pay(fromUid,toUid,amount,ruleId);

                producer.send(new ProducerRecord<String, String>("risk", JSON.toJSONString(pay)));
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }




}