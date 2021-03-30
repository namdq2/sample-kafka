package com.namdq.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private ConsumerDemoWithThread() {
  }

  private void run() {
    String groupId = "my-first-application";
    String topic = "first-topic";

    // latch for dealing with multiple threads
    CountDownLatch countDownLatch = new CountDownLatch(1);

    // create the consumer runnable
    logger.info("Creating the consumer thread");
    ConsumerRunnable consumerRunnable = new ConsumerRunnable(
        groupId,
        topic,
        countDownLatch
    );

    // start the thread
    Thread thread = new Thread(consumerRunnable);
    thread.start();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Caught shutdown hook");
      consumerRunnable.shutdown();
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        logger.info("Application has exited");
      }
    }));

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }

  public class ConsumerRunnable implements Runnable {

    private CountDownLatch countDownLatch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String topic, String groupId, CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;

      // create consumer config
      Properties properties = KafkaConfig.getConsumerProperties();
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      consumer = new KafkaConsumer<>(properties);

      // subscribe consumer to our topic(s)
      consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: {}, value: {}", record.key(), record.value());
            logger.info("Partition: {}, offset: {}", record.partition(), record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("Received shutdown signal.");
      } finally {
        consumer.close();
        // tell our main code we're done with the consumer
        countDownLatch.countDown();
      }
    }

    public void shutdown() {
      // the wakeup() method is a special method to interrupt consumer.poll()
      // it will throw the exception WakeUpException
      consumer.wakeup();
    }
  }
}
