package com.pk.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Implementation of <b>Apache Kafka v2.5</b> <code>Consumer</code> in Java.
 * 
 * @author PranaySK
 * @since 1.0
 */

public class MyKafkaConsumer {
	private static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

	public static void main(String[] args) {
		String bootstrapServers = "localhost:9092";
		String topic = "test";
		String groupId = "myTopicGroup";
		String resetOffset = "earliest";
		// Creating properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetOffset);

		// Creating Consumer
		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {

			// Subscribing the Consumer to topic
			kafkaConsumer.subscribe(Arrays.asList(topic));

			// Polling the data
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(),
							record.partition(), record.offset());
				}
			}
		}
	}
}
