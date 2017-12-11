package com.siemens.ct.brownfield.kafkaM;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.ct.brownfield.kafkaM.consumer.ConsumerTest;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 17, 2017
 *
 */

public class KafkaWorker implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(KafkaWorker.class);

	private ConsumerRecord<String, String> consumerRecord;

	public KafkaWorker(ConsumerRecord<String, String> record) {
		this.consumerRecord = record;
	}

	@Override
	public void run() {
		// process data
		logger.info("{} consumed {}th message with offet: {} ", Thread.currentThread().getName(),
				consumerRecord.partition(), consumerRecord.offset());
	}
}
