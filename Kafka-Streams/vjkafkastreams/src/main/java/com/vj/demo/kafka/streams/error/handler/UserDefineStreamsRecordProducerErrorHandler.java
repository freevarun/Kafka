package com.vj.demo.kafka.streams.error.handler;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class UserDefineStreamsRecordProducerErrorHandler implements ProductionExceptionHandler {
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
		
		if (exception instanceof RecordTooLargeException) {
            // Log and skip the oversized record
            log.warn("*****Skipping oversized record for topic {}: {}", record.topic(), exception.getMessage());
            
         // TODO park this messages to DLQ and continue;
            
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
		// TODO park this messages to DLQ and continue;
        return ProductionExceptionHandlerResponse.CONTINUE;
	}
	
	
	
	
}
