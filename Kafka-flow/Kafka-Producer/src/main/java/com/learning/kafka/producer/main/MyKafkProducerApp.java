package com.learning.kafka.producer.main;

import com.learning.kafka.producer.MyKafkaProducer;
import com.learning.kafka.producer.constants.KafkaProducerConstants;

public class MyKafkProducerApp {

	public static void main(String[] args) {
		MyKafkaProducer kafkaProducer = new MyKafkaProducer();
		kafkaProducer.pushMessageViaKafkaProducer(KafkaProducerConstants.TOPIC_NAME,"Hello world!");
	}

}
