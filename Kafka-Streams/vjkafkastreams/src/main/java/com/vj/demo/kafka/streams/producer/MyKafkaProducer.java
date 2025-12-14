package com.vj.demo.kafka.streams.producer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyKafkaProducer {

	@Value(value = "${spring.kafka.bootstrap-server}")
	private static String bootstrapAddress="localhost:9092";

	@Value(value = "${spring.kafka.producer.topic:my-first-stream-input}")
	private static String producerTopic="my-first-stream-input";
	
	@Value(value = "${spring.kafka.steams.output.topic.1:my-first-stream-output1}")
	private static String producerTopic1="my-first-stream-output1";
	
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		try {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringSerializer");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringSerializer");

			KafkaProducer producer = new KafkaProducer<String, String>(props);

			for (int i = 0; i < 1; i++) {
				ProducerRecord<String, String> data;
				String key = UUID.randomUUID().toString();
				String value = UUID.randomUUID().toString();
				data = new ProducerRecord<String, String>(producerTopic, key, value);
				//data = new ProducerRecord<String, String>(producerTopic, value);
				data.headers()
					.add("msgId", "Varun".getBytes());
				
				RecordMetadata metaDate = (RecordMetadata) producer.send(data).get();
				
				log.info("Record: " + value + " key: " + key + " offset:" + metaDate.offset()
						+ " partition : " + metaDate.partition() + " topic : " + metaDate.topic());
				
				Thread.sleep(1L);
			}

			producer.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}