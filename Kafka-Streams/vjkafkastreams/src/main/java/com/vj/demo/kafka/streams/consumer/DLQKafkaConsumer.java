package com.vj.demo.kafka.streams.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DLQKafkaConsumer {

	@KafkaListener(topics = "${spring.kafka.steams.dlq.topic}", groupId = "${spring.kafka.steams.dlq.topic.group.id}")
	public void consumerRecord(ConsumerRecord<String, String> record) {

		if (record != null && !record.value().isEmpty()) {
			try {
				log.info("DLQKafkaConsumer : Received Record: Topic: " + record.topic() + " Partition: "
						+ record.partition() + " Offset: " + record.offset() + " Key: " + record.key() + " Value: "
						+ record.value());

				String msgId = new String(record.headers().lastHeader("msgId").value());
				log.info("DLQKafkaConsumer: Msgid header value: " + msgId);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
