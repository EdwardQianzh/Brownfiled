package com.siemens.ct.brownfield.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.ct.brownfield.kafkaM.consumer.KafkaConsumerWorker;

/**
 *
 * @Date: 2017年11月18日
 * @Author: Bob He
 *
 */
public class PropertiesUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

	public static Properties getProperty() {
		InputStream is = null;
		Properties property = null;
		try {
			is = PropertiesUtil.class.getClassLoader().getResourceAsStream("application.properties");
			property = new Properties();
			try {
				property.load(is);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return property;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
