package io.conduktor.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        // Create producer properties

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // create a producer record

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello Kafka");

        // Send the data -  async operation

        kafkaProducer.send(producerRecord);

        // flush and close the Producer - synchronous operation

        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
