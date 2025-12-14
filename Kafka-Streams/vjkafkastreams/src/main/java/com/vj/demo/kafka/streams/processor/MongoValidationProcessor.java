package com.vj.demo.kafka.streams.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class MongoValidationProcessor<K, V> implements Processor<K, V>{
	
	private ProcessorContext context;

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
		
	}

	@Override
	public void process(K key, V value) {
		if(key != null & value != null) {
			this.context.forward(key, value);
			log.info("Stream ready for procerssing post filter having key : "+key+" and value : "+value);
		}else {
			log.error("Exception in MongoValidationProcessor");
			throw new RuntimeException("Exception in MongoValidationProcessor");
		}
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}