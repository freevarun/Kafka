package com.vj.demo.kafka.streams.builder;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import com.vj.demo.kafka.streams.config.VJKafkaStreamsConfig;
import com.vj.demo.kafka.streams.processor.MongoValidationProcessor;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@EnableKafka
@EnableKafkaStreams
@Configuration
@Slf4j
public class KafkaTopologyBuilder {

	@Value(value = "${spring.kafka.steams.input.topic}")
    private String inputTopic;
	
	@Value("${spring.kafka.steams.output.topic.1}")
    private String outputTopic1;
	
	@Value("${spring.kafka.steams.output.topic.2}")
    private String outputTopic2;
	
	@Autowired
	StreamsBuilder streamsBuilder;
	
	@Autowired
	VJKafkaStreamsConfig vjKafkaStreamsConfig;
	
	@Autowired
	KafkaStreamsConfiguration kafkaStreamsConfiguration;

    @PostConstruct
    public void build() {
    	Topology topology = streamsBuilder.build();
    	
    	topology
    		.addSource("SOURCE",Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
    		.addProcessor("FILTER_DATA",MongoValidationProcessor<String, String>::new,"SOURCE")
    		.addSink("SINK-1", outputTopic1, Serdes.String().serializer(),Serdes.String().serializer(),"FILTER_DATA")
    		.addSink("SINK-2", outputTopic2, Serdes.String().serializer(),Serdes.String().serializer(),"FILTER_DATA");
    	
    	KafkaStreams streaming = new KafkaStreams(topology,kafkaStreamsConfiguration.asProperties());
    	
    	//streaming.setUncaughtExceptionHandler(new UserDefineStreamsUncaughtExceptionHandler());
    	/*streaming.setUncaughtExceptionHandler(ex -> {
    	    log.error("Kafka-Streams uncaught exception occurred. Stream will be replaced with new thread", ex);
    	    
    	    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
    	});*/
    	streaming.start();
    	
    	Runtime.getRuntime().addShutdownHook(new Thread(streaming::close));

    }

}
