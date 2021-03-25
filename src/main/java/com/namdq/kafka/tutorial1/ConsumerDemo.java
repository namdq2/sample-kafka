package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

  public static void main(String[] args) {
    String groupId = "my-first-application";
    String topic = "first-topic";

    // create consumer config
    Properties properties = KafkaConfig.getConsumerProperties();
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

      // subscribe consumer to our topic(s)
      consumer.subscribe(Collections.singletonList(topic));

      // poll for new data
      while (true) {
        try {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: {}, value: {}", record.key(), record.value());
            logger.info("Partition: {}, offset: {}", record.partition(), record.offset());
          }
        } catch (Exception e) {
          logger.error("Consumer is closing.", e);
          break;
        }
      }
    }

  }
}
