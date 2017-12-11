package com.siemens.ct.brownfield.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 9, 2017
 * 
 */

public class SparkDataStoreProcessorS {

	private final static Logger logger = LoggerFactory.getLogger(SparkDataStoreProcessorS.class);
	private KafkaProducer<String, String> producer;
	private static SparkDataStoreProcessorS instance = null;

	private SparkDataStoreProcessorS(String servers) {
		if (servers == null) {
			return;
		}
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		logger.info("*****create producer successfully!*****");
		this.producer = new KafkaProducer(props);
	}

	// get instance
	public static synchronized SparkDataStoreProcessorS getKafkaProducer(String servers) {
		if (instance == null) {
			instance = new SparkDataStoreProcessorS(servers);
			logger.info("***** initialize kafka producer *****");
		}
		return instance;
	}

	// send data
	public void sendData(ProducerRecord<String, String> data) {
		logger.info("*****data ***** {}", data.value());
		producer.send(data, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null)
					e.printStackTrace();
				logger.info("*****The offset of the record:{}*****", metadata.offset());
			}
		});
	}
	
	public void shutdown() {
	    producer.close();
	  }
}
