package com.pk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of <b>Apache Kafka v2.5</b> <code>Producer</code> with Keys
 * for each Value to be sent to the <code>Consumer</code> in Java.
 * 
 * @author PranaySK
 * @since 1.0
 */

public class MyKafkaProducerWithKey {
	private static final Logger logger = LoggerFactory.getLogger(MyKafkaProducerWithKey.class);

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		String bootstrapServers = "localhost:9092";
		String topic = "test";
		// Creating properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Creating Producer
		try (KafkaProducer<String, String> secondProducer = new KafkaProducer<>(properties)) {

			for (int i = 0; i < 10; i++) {
				String key = "id_" + i;
				String value = "Hello_" + i;
				// Creating a record with key
				ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
				logger.info("key -> {}", key);
				// Sending synchronous data forcefully
				secondProducer.send(record, MyKafkaProducerWithKey::onCompletion).get();
			}
			secondProducer.flush();
		}
	}

	private static void onCompletion(RecordMetadata recordMetadata, Exception ex) {
		if (ex == null) {
			logger.info(
					"Successfully received the details as: \nTopic: {} \nPartition: {} \nOffset: {} \nTimestamp: {}",
					recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
					new Date(recordMetadata.timestamp()));
		} else {
			logger.error("Can't produce! Getting error -> ", ex);
		}
	}
}