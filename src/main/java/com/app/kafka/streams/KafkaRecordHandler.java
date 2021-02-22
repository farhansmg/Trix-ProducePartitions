package com.app.kafka.streams;

import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import jdk.internal.org.jline.utils.Log;

public class KafkaRecordHandler implements Callable<Object> {

	private ConsumerRecord<String, String> record;
	static Logger LOGGER = Logger.getLogger(KafkaRecordHandler.class.getName());
	public KafkaRecordHandler(ConsumerRecord<String, String> record) {
		this.record = record;
	}

	@Override
	public Object call() throws Exception {
		// TODO Auto-generated method stub
		handle();
		return null;
	}
	
	public void handle() { 
   		try { 
   			LOGGER.info("Got message : " + record.value() + " From partitions : " + record.partition());
   		} 
   		catch (Exception e) { 
   			LOGGER.error("Error in sending record : " + e);
   		}
	}

}
