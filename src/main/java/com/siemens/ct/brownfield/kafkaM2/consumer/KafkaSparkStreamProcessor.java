package com.siemens.ct.brownfield.kafkaM2.consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 30, 2017
 *
 */

public class KafkaSparkStreamProcessor {

	private List<KafkaConsumerRunnable> consumerlist = null;

	public KafkaSparkStreamProcessor(int consumerNum, String groupId, String topic, String brokerList,
			IKafkaConsumerWorker handler) {
		consumerlist = new ArrayList<>(consumerNum);
		for (int i = 0; i < consumerNum; ++i) {
			KafkaConsumerRunnable consumerThread = new KafkaConsumerRunnable(brokerList, groupId, topic, handler);
			consumerlist.add(consumerThread);
		}
	}

	public void execute() {
		Thread t = null;
		try {
			for (KafkaConsumerRunnable task : consumerlist) {
				t = new Thread(task);
				t.start();
			}
		} finally {
			consumerlist.clear();
		}
	}
}
