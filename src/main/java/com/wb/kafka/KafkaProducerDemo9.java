package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.OrderTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * flink1.12.0
 */
public class KafkaProducerDemo9 {

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
long orderId = 1;
producer = new KafkaProducer<String, String>(properties);
while (true){
    OrderTable o = new OrderTable();
    o.setId(orderId++);
    o.setShopid(new Random().nextInt(6)+1);
    o.setTs(System.currentTimeMillis());
    producer.send(new ProducerRecord<String, String>("shop_order", JSON.toJSONString(o)));
    Thread.sleep(50);
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