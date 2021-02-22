package com.app.kafka.respond;

public interface KafkaIProducer { 
	public static String KAFKA_BROKERS = "172.17.214.35:9092";
	
	public static String ACKS="0";
	public static String CLIENT_ID="Producer";
	// -------------------------------------------------------
	public static String TOPIC="testingPartition";
}