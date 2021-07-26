package com.learning.kafka.consumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learning.kafka.consumer.constants.KafkaConsumerConstants;

public class MyKafkaConsumer {
	
	Properties properties = null;
	Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class.getName());
	
	public MyKafkaConsumer(){
		
	}
	
	public MyKafkaConsumer(String groupId){
		if(properties==null) {
			properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConsumerConstants.BOOTSTRAP_SERVER);
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
			
			//earliest - read from the beginning
			//latest -> read from new offset
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,KafkaConsumerConstants.AUTO_OFFSET_RESET);
			
			//this is used to disable auto commit of offset
			properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,KafkaConsumerConstants.ENABLE_AUTO_COMMIT);
			
			//this will control the number of records poll at a time
			properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,KafkaConsumerConstants.MAX_POLL_RECORD);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		}
	}
	
	public void bringKafkaConsumerUp(List<String> topicName) {
		KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
		kafkaConsumer.subscribe(topicName);
		
		while(true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("Received "+records.count()+" records");
				String id = record.topic()+"_"+record.partition()+"_"+record.offset();
				
				//this is the id which needed to be identified that will help in understanding to make consumer idempotent
				//we need to make in such a way that if consumer group goes down we will received the same message again 
				//that to at-least once mode
				System.out.println(id);
				logger.info("Key: "+record.key()+", Value: "+record.value());
				logger.info("Partitions: "+record.partition()+", Offset: "+record.offset());
				
				//Commit particular processed message
				TopicPartition topicPartition = new TopicPartition(record.topic(),record.partition());
				OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset());
				Map<TopicPartition, OffsetAndMetadata> mataDataMap = new HashMap<TopicPartition, OffsetAndMetadata>();
				mataDataMap.put(topicPartition, offsetAndMetadata);
				kafkaConsumer.commitSync(mataDataMap);
			}
			
			try {
				Thread.sleep(10);// introduce the delay in processing the record
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
