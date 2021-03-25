package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties())) {
      String topic = "first-topic";

      for (int i = 0; i < 10; i++) {
        String value = "hello world" + i;
        String key = "id_" + i;

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        logger.info("Key: {}", key);
        // id_o is going to partition 1
        // id_1 partition 0
        // id_2 partition 2
        // id_3 partition 0
        // id_4 partition 2
        // id_5 partition 2
        // id_6 partition 0
        // id_7 partition 2
        // id_8 partition 1
        // id_9 partition 2

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
        }).get(); // block the .send() to make it synchronous - don't do this in production!
      }

      // flush data
      producer.flush();
    }
  }
}
