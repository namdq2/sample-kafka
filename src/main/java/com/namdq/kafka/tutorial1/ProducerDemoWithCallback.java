package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

  public static void main(String[] args) {
    String topic = "first-topic";

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties())) {
      for (int i = 0; i < 10; i++) {

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello world " + i);

        // send data - asynchronous
        producer.send(record, (metadata, exception) -> {

          // executes every time a record is successfully sent or an exception is thrown
          if (exception == null) {

            // the record was successfully sent
            logger.info("Received new metadata. \n" +
                    "Topic: {}\n" +
                    "Partition: {}\n" +
                    "Offset: {}\n" +
                    "Timestamp: {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp());
          } else {
            logger.error("Error while processing.", exception);
          }
        });
      }

      // flush data
      producer.flush();
    }
  }
}
