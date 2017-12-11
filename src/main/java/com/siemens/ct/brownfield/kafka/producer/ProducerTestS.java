package com.siemens.ct.brownfield.kafka.producer;
//package com.siemens.ct.brownfield.kafka.producer;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
///**
// * @Author He Bao Jing Z003NUJF
// * @Date Nov 9, 2017
// *
// */
//
//public class ProducerTest {
//
//	public static void main(String[] args) {
//		String topic = "Analyzed";
////		String servers = "10.192.27.231:6667,10.192.27.232:6667";
//		String servers = "192.168.1.11:9092";
//
//		SparkDataStoreProcessor sparkDataStoreProcessor = SparkDataStoreProcessor.getKafkaProducer(servers);
//		
//		for (int i = 0; i < 100000; i++) {
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			String content = "DD" + i;
//			// create record
//			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, content);
//			// send data to broker
//			sparkDataStoreProcessor.sendData(record);
//		}
//	}
//
//}
