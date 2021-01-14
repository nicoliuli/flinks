package com.wb.kafka;

import com.alibaba.fastjson.JSON;
import com.wb.common.WordInput;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducerDemo3 {

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


        String line = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            // 遍历目录下的所有文本文件
            String dirPath = "/Users/liuli/code/wb-mina-root/wb-mina-api/src/main/java/com/wb/mina/controller/";
            List<String> fileList = filePathList(dirPath);
            for (String filePath : fileList) {
                File file = new File(filePath);
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file), "UTF-8");
                BufferedReader br = new BufferedReader(inputStreamReader);
                while ((line = br.readLine()) != null) {
                    // 将数据封装后发送给kafka
                    WordInput input = new WordInput(line, file.getName());
                    producer.send(new ProducerRecord<String, String>("word", JSON.toJSONString(input)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static List<String> filePathList(String dirPath) {
        List<String> filePathList = new ArrayList<>();
        File file = new File(dirPath);
        File[] cFile = file.listFiles();
        for (int i = 0; i < cFile.length; i++) {
            if (cFile[i].isDirectory()) {
                continue;
            }
            String name = cFile[i].getName();
            String realPath = dirPath + name;
            filePathList.add(realPath);
        }
        return filePathList;
    }


}