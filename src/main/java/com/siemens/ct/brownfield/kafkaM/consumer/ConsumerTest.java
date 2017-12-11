package com.siemens.ct.brownfield.kafkaM.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.ct.brownfield.util.PropertiesUtil;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 9, 2017
 *
 */

public class ConsumerTest {

	private final static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

	public static void main(String[] args) {

		Properties properties = PropertiesUtil.getProperty();
		String servers = properties.getProperty("kafka.servers").toString();
		String consumerTopic1 = properties.getProperty("kafka.consumerTopic1").toString();
		String groupid = properties.getProperty("kafka.groupid").toString();
		int threadNumber = Integer.parseInt(properties.get("kafka.consumerTopic1ThreadNum").toString());

		// consumer1
		KafkaSparkStreamProcessor kafkaSparkStreamProcessor1 = new KafkaSparkStreamProcessor();
		KafkaConsumer<String, String> consumer1 = kafkaSparkStreamProcessor1.initConsumer(consumerTopic1, groupid,
				servers);
		kafkaSparkStreamProcessor1.execute(consumer1, threadNumber);

		// consumer2
//		KafkaSparkStreamProcessor kafkaSparkStreamProcessor2 = new KafkaSparkStreamProcessor();
//		KafkaConsumer<String, String> consumer2 = kafkaSparkStreamProcessor2.initConsumer(consumerTopic1, groupid,
//				servers);
//		kafkaSparkStreamProcessor2.execute(consumer2, threadNumber);

		// how to if user wannna another consumer

		// get consumer if need
		// KafkaConsumer<String, String> consumer =
		// kafkaSparkStreamProcessor.getConsumer();

	}
}
