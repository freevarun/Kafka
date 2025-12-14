package com.vj.demo.kafka.streams.processor;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class MongoValidationProcessor implements Processor<String, String, String, String>{
	
	private ProcessorContext<String, String> context;
	
	@Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

	@Override
	public void close() {
	}

	@Override
	public void process(Record<String, String> record) {
		
		try {
			String key = record.key();
			String value = record.value();
			if(key != null & value != null) {
				Headers headers = record.headers();
				byte[] msgIdBytes = headers.lastHeader("msgId").value();
				String msgId = new String(msgIdBytes, StandardCharsets.UTF_8);
				log.info("Stream ready for procerssing post filter having key : "+key+" and value : "+value+" header: msgId :"+msgId);
				context.forward(record);
			}else {
				log.error("Exception in MongoValidationProcessor");
				throw new RuntimeException("Exception in MongoValidationProcessor");
			}
		}catch(Exception e) {
			log.error("Exception while processing MongoValidator");
			throw new RuntimeException("Exception in MongoValidationProcessor");
		}
	}
}