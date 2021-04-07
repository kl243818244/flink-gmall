package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * kafka 工具类
 */
public class MyKafkaUtil {

    private static String kafkaServer = "172.16.6.163:9092";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }

}