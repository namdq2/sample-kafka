package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

  public static void main(String[] args) {
    String topic = "first-topic";

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties())) {
      // create producer record
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, "This is a message from ProducerDemo");

      // send data - asynchronous
      producer.send(record);

      // flush data
      producer.flush();
    }
  }
}
