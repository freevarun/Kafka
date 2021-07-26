package com.learning.kafka.producer.constants;

public class KafkaProducerConstants {

	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String ENABLE_IDEMPOTENCE = "true";
	public static final String ACKS = "all";
	public static final String DELIVERY_TIMEOUT_MS = "12000";
	public static final String MAX_IN_FLIGHT_REQUESTS = "5";
	public static final String COMPRESSION_TYPE = "snappy";
	public static final String LINGER_MS = "20";
	public static final String TOPIC_NAME = "testTopic1";
}
