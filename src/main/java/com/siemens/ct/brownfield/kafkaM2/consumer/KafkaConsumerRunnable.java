package com.siemens.ct.brownfield.kafkaM2.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 30, 2017
 *
 */

public class KafkaConsumerRunnable implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnable.class);

	private final KafkaConsumer<String, String> consumer;
	private IKafkaConsumerWorker handler = null;

	public KafkaConsumerRunnable(String brokerList, String groupId, String topic, IKafkaConsumerWorker handler) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		this.handler = handler;
	}

	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
			if (records != null) {
				for (ConsumerRecord<String, String> record : records) {
					handler.process(record);
				}
			}
		}
	}

}
