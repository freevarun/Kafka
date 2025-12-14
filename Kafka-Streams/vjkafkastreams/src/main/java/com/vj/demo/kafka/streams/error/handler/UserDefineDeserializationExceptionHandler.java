package com.vj.demo.kafka.streams.error.handler;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class UserDefineDeserializationExceptionHandler implements DeserializationExceptionHandler {
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record,
			Exception exception) {
		log.error("*****Deserialization error in topic {} partition {} offset {}: {}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());

		// TODO park this messages to DLQ and continue;
		
		return DeserializationHandlerResponse.CONTINUE;
	}
	
	
	
}
