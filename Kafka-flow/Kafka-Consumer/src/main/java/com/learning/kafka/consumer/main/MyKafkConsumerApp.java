package com.learning.kafka.consumer.main;

import java.util.Arrays;

import com.learning.kafka.consumer.MyKafkaConsumer;
import com.learning.kafka.consumer.constants.KafkaConsumerConstants;

public class MyKafkConsumerApp {

	public static void main(String[] args) {
		MyKafkaConsumer kafkaConsumer = new MyKafkaConsumer(KafkaConsumerConstants.GROUP_ID);
		kafkaConsumer.bringKafkaConsumerUp(Arrays.asList(KafkaConsumerConstants.TOPIC_NAME));
	}

}
