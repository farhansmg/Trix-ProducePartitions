
package com.app.kafka.respond;

import java.util.Properties; 
import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.kafka.clients.producer.Producer; 
import org.apache.kafka.clients.producer.ProducerConfig; 
import org.apache.kafka.common.serialization.LongSerializer; 
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerCreator { 
	public static Producer<String, String> createProducerFlow() { 
		Properties props = new Properties();
		props.put(ProducerConfig.ACKS_CONFIG, KafkaIProducer.ACKS);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIProducer.KAFKA_BROKERS); 
		props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaIProducer.CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props); 
	}
}