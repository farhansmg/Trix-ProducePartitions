package com.app.kafka.consumer;

public interface KafkaIConsumer { 
//	Staging
	public static String KAFKA_BROKERS = "172.17.214.35:9092";
	
	public static Integer MESSAGE_COUNT=1000; 
	public static String CLIENT_ID="client1"; 
	public static String TOPIC="testingPartition";
	public static String GROUP_ID_CONFIG="KafkaGroup"; 
	public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100; 
	public static String OFFSET_RESET_EARLIER="earliest"; 
	public static Integer MAX_POLL_RECORDS=1; 
}