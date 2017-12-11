package com.siemens.ct.brownfield.kafkaM.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.siemens.ct.brownfield.util.PropertiesUtil;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 9, 2017
 *
 */

public class ProducerTest {

	public static void main(String[] args) {
		
		Properties prop = PropertiesUtil.getProperty(); 
		String servers = prop.getProperty("kafka.servers").toString();
		String topic = prop.getProperty("kafka.producerTopic1").toString();

		SparkDataStoreProcessor sparkDataStoreProcessor = SparkDataStoreProcessor.getInstance();
		sparkDataStoreProcessor.initProducer(servers);
		
		long nn = 99999999999l;
		
		for (int i = 0; i < nn; i++) {
			 
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String content = "test12---" + i;
			// create record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "key" + i, content);
			// send data to broker
			 sparkDataStoreProcessor.sendData(record);
		}
		
		
	}

}
