package com.learning.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learning.kafka.producer.constants.KafkaProducerConstants;

public class MyKafkaProducerusingStream {
	Properties properties = null;
	Logger logger = LoggerFactory.getLogger(MyKafkaProducerusingStream.class.getName());
	
	public MyKafkaProducerusingStream(){
		if(properties==null) {
			properties = new Properties();
			
			properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaProducerConstants.APPLICATION_ID);
			properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaProducerConstants.BOOTSTRAP_SERVER);
			
			properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
			properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
			properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KafkaProducerConstants.COMMIT_INTERVAL_MS);
			
			//create safe producer
			//
			//this property is used to check if message send by producer is duplicate message or not
			//it its a duplicate message then kafka assume that ack send back to producer is not received by producer
			//so rather than treating that as a new message kafka will send the ack again back to producer
			//this property will be used if Kafka >=0.11
			//properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,KafkaProducerConstants.ENABLE_IDEMPOTENCE);
			
			//ACK=0 -> no response from Kafka topic leader
			//ACK=1 -> response from kafka topic leader without considering replicating factor topic get the message or not
			//ACK=all -> response from kafka topic when message is received by leader and replicating factor both 
			//properties.setProperty(ProducerConfig.ACKS_CONFIG, KafkaProducerConstants.ACKS);
			
			//retry will happen till the time delivery timeout is not reached or retry count max value is not reached
			//properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
			
			//default value for delivery timeout is 2ms -> 120000
			//properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,KafkaProducerConstants.DELIVERY_TIMEOUT_MS);
			
			//this is number of message producer can send to kafka parallely by default its value is 5 for kafka >=1.1
			//if Kafka 2.0 >=1.1 use the below value as 5 otherwise use the value as 1
			//properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
			//															, KafkaProducerConstants.MAX_IN_FLIGHT_REQUESTS);
			
			//high throughput producer (at the expense of a bit of latency and CPU usage)
			//
			//snappy -> this compression is helpful if message is text base like JSON
			//snappy -> good balance of CPU and compression ratio
			//snappy -> created by Google
			// no change is required at consumer end, kafka themself compress, batch and decompress the messages while sending to consumer
			//TODO For now commenting as this consume high CPU, uncomment it out once required
			//properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,KafkaProducerConstants.COMPRESSION_TYPE);
			
			//amount of time producer will wait in order to create the batch,
			//if batch size is reached before linger millisecond then batch will be send 
			//other wise number of message comes in batch will be send after linger millisecond time is reached
			//properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,KafkaProducerConstants.LINGER_MS);
			
			//32 KB batch size
			//properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
			
			
		}
	}
	
	public void pushMsgViaKafkaProducerUsingStream(String topicName, String message) {
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String,String> kStream = streamsBuilder.stream(topicName);
		Topology topology = streamsBuilder.build();
		
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
		kafkaStreams.start();
	}
}

