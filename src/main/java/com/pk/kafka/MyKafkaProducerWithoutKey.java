package com.pk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

/**
 * Implementation of <b>Apache Kafka v2.5</b> <code>Producer</code> without Key
 * for Value to be sent to <code>Consumer</code> in Java.
 * 
 * @author PranaySK
 * @since 1.0
 */

public class MyKafkaProducerWithoutKey {
	private static final Logger logger = LoggerFactory.getLogger(MyKafkaProducerWithoutKey.class);

	public static void main(String[] args) {
		// Creating properties
		String bootstrapServers = "localhost:9092";
		String topic = "test";
		String value = "Hello Kafka Callback!";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Creating Producer
		try (KafkaProducer<String, String> firstProducer = new KafkaProducer<>(properties)) {

			// Creating a record
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

			// Sending data
			firstProducer.send(record, (recordMetadata, ex) -> {
				if (ex == null) {
					logger.info(
							"Successfully received the details as: \nTopic: {} \nPartition: {} \nOffset: {} \nTimestamp: {}",
							recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
							new Date(recordMetadata.timestamp()));
				} else {
					logger.error("Can't produce! Getting error -> ", ex);
				}
			});
			firstProducer.flush();
		}
	}
}