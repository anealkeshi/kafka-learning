package me.anilkc.kafka.producers;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class AsyncProducer {

  public static void main(String[] args) throws Exception {
    String topicName = "AsyncProducerTopic";
    String key = "key";
    String value = "This is a message from Async Producer";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

    producer.send(record, new AsyncProducerCallback());
    System.out.println("Message sent from Async Producer");
    producer.close();

  }

}


class AsyncProducerCallback implements Callback {

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
    if (recordMetadata != null) {
      System.out.println("Call success");
    } else {
      System.out.println("Exception encountered");
    }
  }
}
