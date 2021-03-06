package com.namdq.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.namdq.kafka.tutorial1.KafkaConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  private String consumerKey = "5up9RFifqtdmcWdyUdXnW6Fvk";
  private String consumerSecret = "fOpFmiN30OZetmxbxoMtP9z7Eka05vaNhSkXYm2abGo51p290R";
  private String token = "1163312790880153600-UjtHZt03pguZnju6HFHKZBsKCyEY8p";
  private String secret = "TDgUyHpaluOkTy6kb7TJEy2Bs7zIyZuMTikfSVn22gbwc";

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public TwitterProducer() {
  }

  public void run() {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

    // create a twitter client
    Client client = createTwitterClient(msgQueue);

    // Attempts to establish a connection.
    client.connect();

    // create a kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("stopping application...");
      logger.info("shutting down client from twitter...");
      client.stop();
      logger.info("closing producer...");
      producer.close();
      logger.info("done!");
    }));

    // loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(new ProducerRecord<>("twitter-tweets", null, msg), (metadata, exception) -> {
          if (exception != null) {
            logger.error("Something had happened.", exception);
          }
        });
      }
    }
    logger.info("End of application");
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    return new KafkaProducer<>(KafkaConfig.getProducerProperties());
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    List<String> terms = Lists.newArrayList("bitcoin");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }
}
