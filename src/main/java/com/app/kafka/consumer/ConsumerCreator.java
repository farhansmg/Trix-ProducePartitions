package com.app.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCreator {
    public static Consumer<String, String> createFlowConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIConsumer.KAFKA_BROKERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaIConsumer.CLIENT_ID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaIConsumer.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaIConsumer.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaIConsumer.OFFSET_RESET_EARLIER);
        
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition0 = new TopicPartition(KafkaIConsumer.TOPIC, 0);
        TopicPartition partition1 = new TopicPartition(KafkaIConsumer.TOPIC, 1);
        TopicPartition partition2 = new TopicPartition(KafkaIConsumer.TOPIC, 2);
        consumer.assign(Arrays.asList(partition0,partition1,partition2));
        return consumer;
    }
}