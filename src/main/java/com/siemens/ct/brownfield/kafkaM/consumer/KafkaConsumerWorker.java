package com.siemens.ct.brownfield.kafkaM.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.ct.brownfield.kafkaM.producer.SparkDataStoreProcessor;
import com.siemens.ct.brownfield.util.PropertiesUtil;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 17, 2017
 *
 */

public class KafkaConsumerWorker implements Runnable {

	private ConsumerRecord<String, String> consumerRecord = null;
	private ProducerRecord<String, String> producerRecord = null;

	Properties properties = PropertiesUtil.getProperty();
	String servers = properties.getProperty("kafka.servers");
	String ProducerTopic1 = properties.getProperty("kafka.producerTopic1");

	public KafkaConsumerWorker(ConsumerRecord<String, String> record) {
		this.consumerRecord = record;
	}

	@Override
	public void run() {

		// process data
		// processData();
		System.out.println("***** thread: " + Thread.currentThread().getName() 
				+ " partition: " + consumerRecord.partition() 
				+ " offet: " + consumerRecord.offset() 
				+ "message: " +  consumerRecord.value() + "*****");
		
		
		/**
		 * 1. 订阅多个主题，多个分区 2. 测试代码 3. 测试性能
		 * 
		 * 4. producer 可以指定分区发送，若不指定，就全部分区发送？全部发送按照分区1,2,3发送。接收也是1，2，3
		 * https://www.cnblogs.com/gnivor/p/5318319.html
		 * http://blog.csdn.net/zoubf/article/details/51213550 按照顺序往分区发送
		 * 5. 先确保kafka已经配置了分区数，才能支持多线程收/发
		 * 6. consumer多线程消费 确定使用方案：①一个consumer，多个处理线程，没有消息顺序的问题？。 ②每个线程维护consumer
		 * 7. partition个数在创建topic的时候确定  与 broker有关，
		 * 
		 */

		SparkDataStoreProcessor sparkDataStoreProcessor = SparkDataStoreProcessor.getInstance();
		sparkDataStoreProcessor.initProducer(servers);
//
//		/**
//		 * http://blog.csdn.net/jsky_studio/article/details/41990841
//		 * -- ProducerRecord(topic, partition, key, value)
//		 * 
//		 * -- ProducerRecord(topic, key, value)
//		 * 
//		 * -- ProducerRecord(topic, value)
//		 */
//		// 指定key，随机发送
		producerRecord = new ProducerRecord<String, String>(ProducerTopic1, consumerRecord.key() , consumerRecord.value());
		sparkDataStoreProcessor.sendData(producerRecord);
	}
}
