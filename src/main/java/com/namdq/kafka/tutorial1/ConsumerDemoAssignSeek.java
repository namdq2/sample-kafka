package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

  public static void main(String[] args) {
    String topic = "first-topic";

    // create consumer config
    Properties properties = KafkaConfig.getConsumerProperties();
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

      // assign and seek are mostly used to replay data or fetch a specific message

      TopicPartition topicPartition = new TopicPartition(topic, 0);
      long offsetToReadFrom = 15L;
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seek(topicPartition, offsetToReadFrom);

      int numberOfMessagesToRead = 5;
      boolean keepOnReading = true;
      int numberOfMessagesReadSoFar = 0;

      // poll for new data
      while (keepOnReading) {
        try {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

          for (ConsumerRecord<String, String> record : records) {
            numberOfMessagesReadSoFar += 1;
            logger.info("Key: {}, value: {}", record.key(), record.value());
            logger.info("Partition: {}, offset: {}", record.partition(), record.offset());
            if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
              keepOnReading = false;
              break;
            }
          }
        } catch (Exception e) {
          logger.error("Consumer is closing.", e);
          break;
        }
      }
    }

  }
}
