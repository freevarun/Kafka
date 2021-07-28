package com.learning.kafka.producer.main;

import com.learning.kafka.producer.MyKafkaProducer;
import com.learning.kafka.producer.MyKafkaProducerusingStream;
import com.learning.kafka.producer.constants.KafkaProducerConstants;

public class MyKafkProducerApp {

	public static void main(String[] args) {
		MyKafkaProducer kafkaProducer = new MyKafkaProducer();
		kafkaProducer.pushMessageViaKafkaProducer(KafkaProducerConstants.TOPIC_NAME,"Hello world!");
	}
	
	//kafka Producer using Stream
	public static void main1(String[] args) {
		MyKafkaProducerusingStream kafkaProducer = new MyKafkaProducerusingStream();
		kafkaProducer.pushMsgViaKafkaProducerUsingStream(KafkaProducerConstants.TOPIC_NAME,"Hello world!");
	}

}
