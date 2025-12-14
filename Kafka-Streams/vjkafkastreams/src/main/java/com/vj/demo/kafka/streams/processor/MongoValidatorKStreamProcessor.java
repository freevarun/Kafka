package com.vj.demo.kafka.streams.processor;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class MongoValidatorKStreamProcessor {
	
	public KStream<String, String> process(KStream<String, String> stream){
		
		return stream
			.filter((key,value) -> key != null)
			.peek((key,value) -> log.info("Stream with not null key : "+key+" and value : "+value))
			.filter((key,value) -> value !=null && !value.isEmpty())
			.peek((key,value) -> log.info("Stream with not null value having key : "+key+" and value : "+value))
			.filter(new Predicate<String, String>() {
				@Override
				public boolean test(String key, String object) {				 				 
					
					//TODO Validate data in MonoDB
					return true;
				}			 
			})
			.peek((key,value) -> log.info("Stream ready for procerssing post filter having key : "+key+" and value : "+value));
		
    }
}
