package me.anilkc.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicProducer {

  public static void main(String[] args) throws Exception {

    String topicName = "BasicProducerTopic";
    String key = "key";
    String value = "This is a message";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
    producer.send(record);
    producer.close();
  }
}
