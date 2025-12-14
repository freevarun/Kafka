package com.vj.demo.kafka.streams.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.vj.demo.kafka.streams.error.handler.UserDefineDeserializationExceptionHandler;
import com.vj.demo.kafka.streams.error.handler.UserDefineProcessingErrorHandler;
import com.vj.demo.kafka.streams.error.handler.UserDefineStreamsRecordProducerErrorHandler;

import lombok.extern.slf4j.Slf4j;

@EnableKafka
@EnableKafkaStreams
@Configuration
@Slf4j
public class VJKafkaConsumerConfig {

	@Value(value = "${spring.kafka.bootstrap-server}")
	private String bootstrapAddress;

	@Value("${spring.kafka.steams.application.id:streams-app}")
	private String applicationId;

	@Value("${spring.kafka.steams.auto.offset.reset:earliest}")
	private String autoOffderReset;

	@Value("${spring.kafka.consumer.group.id1:groupId1}")
	private String consumerGroupId;
	
	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		//props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				(record, exception) -> handleConsumerError(record, exception)
							, new FixedBackOff(1000L, 3) // Retry every 1 second, max 3 attempts
		);

		factory.setCommonErrorHandler(errorHandler);

		return factory;
	}
	
	private void handleConsumerError(ConsumerRecord<?, ?> record, Exception exception) {
	    String topic = record.topic();
	    int partition = record.partition();
	    long offset = record.offset();

	    //Throwable rootCause = getRootCause(exception);
	    //String errorType = classifyError(rootCause);

	    log.error("*****Kafka error [type={}, topic={}, partition={}, offset={}]: {}",
	              "errorType", topic, partition, offset, exception.getMessage(), exception);

	    // Tag for observability / monitoring systems (e.g., Rollbar, Datadog, etc.)
	    //monitoringService.notify(exception, String.format("Kafka error on topic %s [type=%s, offset=%d]", topic, errorType, offset));

	    // Forward to DLT with enriched metadata
	    //forwardToDeadLetterTopic(record, exception, errorType);
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfig() {
		
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffderReset);
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
		

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, UserDefineDeserializationExceptionHandler.class);
		props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, UserDefineStreamsRecordProducerErrorHandler.class);
		props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, UserDefineProcessingErrorHandler.class);
		
		
		KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration(props);
		return kafkaStreamsConfiguration;
	}
	
}