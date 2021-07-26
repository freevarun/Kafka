package com.learning.kafka.consumer;

import java.util.List;
import java.util.Properties;

public class KafkaConsumer {
	
	Properties properties = null;
	
	public KafkaConsumer(){
		
	}
	
	public KafkaConsumer(List<String> topicName, String groupId){
		if(properties==null) {
			properties = new Properties();
		}
		//properties.setProperty(Consumer,  value)
	}

}
