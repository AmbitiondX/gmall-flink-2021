package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static Properties properties = new Properties();
    private static final String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    static {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
    }

    /**
     * 获取消费者对象
     * @param topic 消费的主题
     * @param groupId 消费者组id
     * @return
     */
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId) {
        // 添加消费者属性
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 获取生产者对象
     * @param topic kafka主题
     * @return
     */
    public static FlinkKafkaProducer getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer(topic, new SimpleStringSchema(), properties);
    }

    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer(
                DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );
    }



}
