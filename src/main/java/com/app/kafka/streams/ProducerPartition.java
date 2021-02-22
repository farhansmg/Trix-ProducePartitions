package com.app.kafka.streams;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import com.app.kafka.consumer.ConsumerCreator;
import com.app.kafka.respond.KafkaIProducer;
import com.app.kafka.respond.ProducerCreator;

import jdk.internal.org.jline.utils.Log;

public class ProducerPartition implements Runnable{
	private ExecutorService exec = Executors.newSingleThreadExecutor();
	private ExecutorService execHandler;
	private static ProducerPartition myself;
	private boolean isRunning = true;
	private Producer<String, String> producer;
	private ProducerRecord<String, String> producer_record1;
	static Logger LOGGER = Logger.getLogger(ProducerPartition.class.getName());
	
	public ProducerPartition() {
		producer = ProducerCreator.createProducerFlow();
		
		int threads = Runtime.getRuntime().availableProcessors();
		execHandler = Executors.newFixedThreadPool(threads);
		myself = this;
	}
	
	public static ProducerPartition get() {
		return myself;
	}

	public ProducerPartition start() {
		exec.submit(this);
		return this;
	}
	@Override
	public void run() {
		try {
			Integer partition = 0;
			while (isRunning) {
				Thread.sleep(1000);
				if(partition == 3) {
					partition = 0;
				}
				String message = "This message belongs to : " + partition.toString();
				this.producer_record1 = new ProducerRecord<String, String>(KafkaIProducer.TOPIC, partition, "message", message);
	   			producer.send(producer_record1);
	   			partition += 1;
			}
		} catch (WakeupException e) {
			LOGGER.error("Processor WakeupException : ", e);
			// Ignore exception if closing
		} catch (Exception e) {
			LOGGER.error("KafkaProcessor Fe CRASHED !!!", e);
			// TODO just find a way to restart this thread if we can't fix the problem
			// is this the right way to restart the consumer whenever it crashed ?
		}
		isRunning = false;
		return;
	}
	
	public void shutdown() {
		if (exec != null) {
			exec.shutdown();
			exec = null;
		}
		if (producer != null) {
			producer.flush();
			producer.close();
		}
		if(execHandler != null) { 
			execHandler.shutdown();
			execHandler = null;
		}
		myself = null;
	}
}
