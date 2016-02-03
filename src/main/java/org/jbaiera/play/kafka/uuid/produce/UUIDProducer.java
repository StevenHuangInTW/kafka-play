package org.jbaiera.play.kafka.uuid.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class UUIDProducer {

    private void run() throws InterruptedException {

        String topic = "t1";

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("buffer.memory", "33554432");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        while (true) {
            String value = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>(topic, value, value));
        }
    }

    public static void main(String[] args) throws Exception {
        new UUIDProducer().run();
    }
}
