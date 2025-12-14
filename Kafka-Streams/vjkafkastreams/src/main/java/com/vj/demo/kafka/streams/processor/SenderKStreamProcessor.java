package com.vj.demo.kafka.streams.processor;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class SenderKStreamProcessor {
	
	public void process(KStream<String, String> stream, String topicName){
		
    	stream.to(topicName);
    	stream.peek((key,value) -> log.info("Publish data on topic: "+topicName+" having key:"+key+" value: "+value)); 	
    	
    }
}
