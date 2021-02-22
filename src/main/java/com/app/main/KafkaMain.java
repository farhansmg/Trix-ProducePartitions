package com.app.main;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.File;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.app.kafka.streams.ConsumerAssign;
import com.app.kafka.streams.ProducerPartition;

public class KafkaMain {
	static Logger LOGGER = Logger.getLogger(KafkaMain.class.getName());
	public static Hashtable<Long,Thread> threads = new Hashtable<Long,Thread>();
	// main function
	public static void main(String args[]) {
		String log4jConfPath = System.getProperty("user.dir")+File.separatorChar+"log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		LOGGER.info("Application Starting");
		int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService processor = Executors.newFixedThreadPool(threads);
		ExecutorService processor2 = Executors.newFixedThreadPool(threads);
		try {
			processor2.submit(new ConsumerAssign());
			processor.submit(new ProducerPartition());
		} 
		catch (Exception e) {
			e.printStackTrace();
			processor.shutdown();
		}
	}

}