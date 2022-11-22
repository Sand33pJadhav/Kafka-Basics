package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-first-application";
        String topic = "demo_java";

        //create consumer configs

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //create consumer

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread

        final Thread mainThread = Thread.currentThread();

        // addind shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown signal, let's exit by calling consumer.wakeup()...");
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread

            try {
                mainThread.join();
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }));

        try{
            // subscribe consumer to our topic

            kafkaConsumer.subscribe(Arrays.asList(topic));

            // poll for new data
            while (true){

                log.info("Polling");

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: consumerRecords){
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        }
        catch (WakeupException e){
            log.info("wake up exception!");
            // ignore this expected exception
        }
        catch (Exception e){
            log.info("Unexpected exception");
        }
        finally {
            // this will also commit the offset if required
            kafkaConsumer.close();
        }

    }
}
