package com.siemens.ct.brownfield.kafka.consumer;
/**
* @Author He Bao Jing Z003NUJF
* @Date Nov 9, 2017
*
*/

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * config kafka consumer
 * 
 * @author Z003NUJF
 * 
 */
public class KafkaSparkStreamProcessorS {

	public static Logger logger = LoggerFactory.getLogger(KafkaSparkStreamProcessorS.class);

	private KafkaConsumerHandlerS consumerHandler;
	private KafkaConsumer<String, String> consumer = null;

	// initialize the consumer configuration
	public KafkaSparkStreamProcessorS(String topic, String groupid, String servers,
			KafkaConsumerHandlerS consumerHandler) {
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("group.id", groupid);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		logger.info("*****create consumer successfully!*****");
		consumer.subscribe(Arrays.asList(topic));
		this.consumerHandler = consumerHandler;
	}

	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}

	// send data to caller
	public void getData(KafkaConsumer<String, String> consumer) {
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(200);
				if (records == null) {
					logger.info("*****records is null!*****");
					return;
				}
				for (ConsumerRecord<String, String> record : records) {
					logger.info("***********send data!***********");
					consumerHandler.process(record);
				}
			}
		} finally {
			consumer.close();
		}
	}
}
