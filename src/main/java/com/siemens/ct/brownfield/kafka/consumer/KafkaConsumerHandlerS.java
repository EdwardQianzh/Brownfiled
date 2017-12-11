package com.siemens.ct.brownfield.kafka.consumer;
/**
* @Author He Bao Jing Z003NUJF
* @Date Nov 13, 2017
*
*/

public interface KafkaConsumerHandlerS<T> {
	public void process(T record);
}
