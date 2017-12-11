package com.siemens.ct.brownfield.kafkaM.producer;

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

public class SparkDataStoreProcessor {

	private final static Logger logger = LoggerFactory.getLogger(SparkDataStoreProcessor.class);
	private KafkaProducer<String, String> producer;

	/**
	 * initial class and get producer
	 * 
	 * @param servers
	 */
	public void initProducer(String servers) {
		if (producer == null) {
			if (servers == null) {
				return;
			}
			Properties props = new Properties();
			props.put("bootstrap.servers", servers);
			props.put("acks", "all");
			props.put("retries", 1);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			logger.info("***** create producer successfully! *****");
			producer = new KafkaProducer<String, String>(props);
		}
	}

	private SparkDataStoreProcessor() {}

	/**
	 * get producer instance
	 * 
	 * @param
	 * @return
	 */
	public static synchronized SparkDataStoreProcessor getInstance() {
		return new SparkDataStoreProcessor();
	}

	/**
	 * producer send data
	 * 
	 * @param data
	 */
	public void sendData(ProducerRecord<String, String> data) {
		producer.send(data, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null)
					e.printStackTrace();
				logger.info("***** Producer offset:{}, topic:{}, partition:{} *****"
						, metadata.offset(), metadata.topic(),metadata.partition());
			}
		});
	}

	/**
	 * close producer
	 */
	public void shutdown() {
		producer.close();
	}
}
