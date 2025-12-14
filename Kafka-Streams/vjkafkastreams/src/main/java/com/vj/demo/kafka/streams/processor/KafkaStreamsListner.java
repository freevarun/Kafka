package com.vj.demo.kafka.streams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import lombok.extern.slf4j.Slf4j;

@EnableKafka
@EnableKafkaStreams
@Configuration
@Slf4j
public class KafkaStreamsListner {
	
	@Value(value = "${spring.kafka.steams.input.topic}")
    private String inputTopic;
	
	@Value("${spring.kafka.steams.output.topic.1}")
    private String outputTopic1;
	
	@Value("${spring.kafka.steams.output.topic.2}")
    private String outputTopic2;
	
	@Autowired
	private MongoValidatorKStreamProcessor mongoValidatorKStreamProcessor;
	
	@Autowired
	private SenderKStreamProcessor senderKStreamProcessor;
	
	//@Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
    	
        KStream<String, String> stream = kStreamBuilder
        		.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        
        //Process KStream
        KStream<String, String> validatedMongoStream = this.mongoValidatorKStreamProcessor.process(stream);  
        this.senderKStreamProcessor.process(validatedMongoStream, outputTopic1);
        this.senderKStreamProcessor.process(validatedMongoStream, outputTopic2);
        
        return stream;
    }

}
