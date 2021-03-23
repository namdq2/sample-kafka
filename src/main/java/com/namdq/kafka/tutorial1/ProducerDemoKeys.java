package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
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
      String topic = "first-topic";
      String value = "hello world" + i;
      String key = "id_" + i;

      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("Key: " + key);
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
      }).get(); // block the .send() to make it synchronous - don't do this in production!
    }

    // flush data
    producer.flush();

    //flush and close producer
    producer.close();
  }
}
