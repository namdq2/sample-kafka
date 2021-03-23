package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

  private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

  public static void main(String[] args) {
    String bootstrapServer = "localhost:9092";

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      // create producer record
      ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "Hello world " + i);

      // send data - asynchronous
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          // executes every time a record is successfully sent or an exception is thrown
          if (exception == null) {
            // the record was successfully sent
            logger.info("Received new metadata. \n" +
                "Topic: " + metadata.topic() + "\n" +
                "Partition: " + metadata.partition() + "\n" +
                "Offset: " + metadata.offset() + "\n" +
                "Timestamp: " + metadata.timestamp());
          } else {
            logger.error("Error while processing.", exception);
          }
        }
      });
    }

    // flush data
    producer.flush();

    //flush and close producer
    producer.close();
  }
}
