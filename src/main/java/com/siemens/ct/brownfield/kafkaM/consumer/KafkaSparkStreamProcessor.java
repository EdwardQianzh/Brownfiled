package com.siemens.ct.brownfield.kafkaM.consumer;
/**
* @Author He Bao Jing Z003NUJF
* @Date Nov 9, 2017
*
*/

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public class KafkaSparkStreamProcessor {

	public static Logger logger = LoggerFactory.getLogger(KafkaSparkStreamProcessor.class);

	private ExecutorService executors = null;

	/**
	 * Initialize the consumer configuration
	 * 
	 * @param topic
	 * @param groupid
	 * @param servers
	 */
	public KafkaConsumer<String, String> initConsumer(String topic, String groupid, String servers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("group.id", groupid);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		logger.info("***** create consumer successfully! *****");
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	/**
	 * execute the task with workerNum thread
	 * 
	 * @param workerNum
	 */
	public void execute(KafkaConsumer<String, String> consumer, int workerNum) {
		
		// start another thread to listen on kafka message 
		Thread listenThread = new Thread(new Runnable() {
			public void run() {
				try {
					executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
							new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
					while (true) {
						ConsumerRecords<String, String> records = consumer.poll(200);
						for (final ConsumerRecord<String, String> record : records) {
							executors.submit(new KafkaConsumerWorker(record));
						}
					}
				} finally {
					// close the consumer and executor
					if (consumer != null) {
						consumer.close();
					}
					if (executors != null) {
						executors.shutdown();
					}
				}
			}
		});
		listenThread.start();
	}

}
