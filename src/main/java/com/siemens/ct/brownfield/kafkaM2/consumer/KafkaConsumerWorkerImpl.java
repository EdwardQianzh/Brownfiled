package com.siemens.ct.brownfield.kafkaM2.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* @Author He Bao Jing Z003NUJF
* @Date Nov 30, 2017
*
*/

public class KafkaConsumerWorkerImpl implements IKafkaConsumerWorker<ConsumerRecord> {

	private final static Logger logger = LoggerFactory.getLogger(KafkaConsumerWorkerImpl.class);
	
	@Override
	public void process(ConsumerRecord record) {
    	logger.info("THread: {}, partition: {}, offset: {}", Thread.currentThread().getName(), record.partition(), record.offset());
	}

}
