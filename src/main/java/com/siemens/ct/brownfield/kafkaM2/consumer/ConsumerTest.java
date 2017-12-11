package com.siemens.ct.brownfield.kafkaM2.consumer;

import java.util.Properties;

import com.siemens.ct.brownfield.util.PropertiesUtil;

/**
 *
 * @Date: 2017年11月29日
 * @Author: Bob He
 *
 */
public class ConsumerTest {

	public static void main(String[] args) {

		Properties properties = PropertiesUtil.getProperty();
		String servers = properties.getProperty("kafka.servers").toString();
		String consumerTopic1 = properties.getProperty("kafka.consumerTopic1").toString();
		String groupid = properties.getProperty("kafka.Topic1Groupid").toString();
		int threadNumber = Integer.parseInt(properties.get("kafka.consumerTopic1ThreadNum").toString());
		
		KafkaSparkStreamProcessor consumerGroup = new KafkaSparkStreamProcessor(threadNumber, groupid, consumerTopic1, servers, new  KafkaConsumerWorkerImpl());
        consumerGroup.execute();
	}
}
