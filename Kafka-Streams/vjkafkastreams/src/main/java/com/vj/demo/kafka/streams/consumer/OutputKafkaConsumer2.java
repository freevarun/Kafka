package com.vj.demo.kafka.streams.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class OutputKafkaConsumer2 {
	
	/** READ FROM TEXAS KAFKA TOPIC **/
	@KafkaListener(topics = "${spring.kafka.steams.output.topic.2}"
			,groupId = "${spring.kafka.consumer.group.id2}")
	public void readRxClaimStream(@Payload String record) {
		 
		if(record!=null && record.length()>0) {			 
			try {				 
				//DO ADDITIONAL PROCESSING WITH THIS FILTERED STREAM OF TEXAS SALES. FOR NOW JUST PRINTING IT OUT
				log.info("Consumed Record from OutputKafkaConsumer2 => " + record);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}		
	}
}
