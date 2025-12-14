package com.vj.demo.kafka.streams.error.handler;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class UserDefineProcessingErrorHandler implements ProcessingExceptionHandler {
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
		
		log.error("ProcessingExceptionHandler triggered");
		String rec = (String) record.value();
		String key = (String) record.key();
		Headers kafkaheaders = record.headers();
		byte[] messagesId = kafkaheaders.lastHeader("msgId").value();
		String msgId = new String(messagesId, StandardCharsets.UTF_8);
		
		log.info("Record : "+rec+" key: "+key+" msgId Header :"+msgId);
		
		return ProcessingHandlerResponse.CONTINUE;
	}
	
	
}
