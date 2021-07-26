package com.learning.kafka.consumer;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.learning.kafka.consumer.constants.KafkaConsumerConstants;

public class KafkaConsumer {
	
	Properties properties = null;
	
	public KafkaConsumer(){
		
	}
	
	public KafkaConsumer(List<String> topicName, String groupId){
		if(properties==null) {
			properties = new Properties();
		}
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  KafkaConsumerConstants.BOOTSTRAP_SERVER);
	}

}
