package com.siemens.ct.brownfield.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jtransforms.fft.DoubleFFT_1D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.ct.brownfield.kafka.producer.SparkDataStoreProcessorS;
import com.siemens.ct.brownfield.util.DataFormatUtil;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 9, 2017
 *
 */

public class ConsumerTestS implements KafkaConsumerHandlerS<ConsumerRecord> {

	private final static Logger logger = LoggerFactory.getLogger(ConsumerTestS.class);

	static String ConsumerTopic = "Raw";
	static String ProducerTopic = "Analyzed";
	static String groupid = "consumer.da.spark";
	static String servers = "localhost:9092";
	// producer
	SparkDataStoreProcessorS sparkDataStoreProcessor = SparkDataStoreProcessorS.getKafkaProducer(servers);
	ProducerRecord<String, String> producerRecord = null;

	// get data from consumer
	@Override
	public void process(ConsumerRecord consumerRecord) {
		if (consumerRecord == null) {
			return;
		}
		final String rawDataStr = consumerRecord.value().toString();

		// final double[] data = DataFormatUtil.Str2DoubleArray(rawDataStr,
		// "\n");
		String str1 = rawDataStr.replace("[", "").replace("]", "");
		final double[] data = DataFormatUtil.Str2DoubleArray(str1, ", ");

		DoubleFFT_1D fft = new DoubleFFT_1D(data.length);
		fft.realForward(data);

		final String processedData = DataFormatUtil.FloatArray2Str(data, ", ");
		String res = "[" + processedData + "]";

		producerRecord = new ProducerRecord<String, String>(ProducerTopic,consumerRecord.key().toString(), res);
		sparkDataStoreProcessor.sendData(producerRecord);
		logger.info("*****produce data to kafka: {} ***** ", producerRecord.value());
	}

	public Double[] Str2DoubleArray(String str, String token) {
		final String[] strArray = str.split(token);
		final Double[] doubleArray = new Double[strArray.length];

		int i = 0;
		for (String s : strArray) {
			doubleArray[i++] = Double.valueOf(s);
		}
		return doubleArray;
	}

	public static void main(String[] args) {

		KafkaSparkStreamProcessorS kafkaSparkStreamProcessor = new KafkaSparkStreamProcessorS(ConsumerTopic, groupid,
				servers, new ConsumerTestS());
		KafkaConsumer<String, String> consumer = kafkaSparkStreamProcessor.getConsumer();
		kafkaSparkStreamProcessor.getData(consumer);

	}
}
