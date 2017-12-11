package com.siemens.ct.brownfield.kafkaM2.consumer;
/**
* @Author He Bao Jing Z003NUJF
* @Date Nov 30, 2017
*
*/

public interface IKafkaConsumerWorker<T> {
	public void process(T record);
}
