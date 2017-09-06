package me.anilkc.kafka.producers;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SynchronousProducer {

  public static void main(String[] args) throws Exception {

    String topicName = "SynchronousProducerTopic";
    String key = "key";
    String value = "This is a topic";

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

    try {
      Future<RecordMetadata> metadata = producer.send(record);
      System.out.printf("Metadata: partition: %s, offset: %s", metadata.get().partition(), metadata.get().offset());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }
}
