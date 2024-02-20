package com.trade_calc.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WriteToKafka {

    public static void main(String[] args) {
        new WriteToKafka();
    }

    WriteToKafka() {
        startPublishing();
    }

    private void startPublishing() {

        Thread bloom = new Thread(() -> {
            String topicName = "bloomberg";

            String trade_names[] = {"microsoft", "tesla", "bhp", "vanguard", "blackrock"};
            Random t_name_random = new Random(0);
            Random price_random = new Random(1);

            try (KafkaProducer<Long, String> producer = new KafkaProducer<>(kafkaProps())) {
                for (long eventCount = 0; ; eventCount++) {
                    String trade_name = trade_names[t_name_random.nextInt(5)];
                    int price = price_random.nextInt(10)*100;
                    String time = DateTime.now().toString();

                    String trade = "bloomberg-"+trade_name+","+price+","+time;
                    producer.send(new ProducerRecord<>(topicName, eventCount, trade));
                    System.out.format("Published '%s' to Kafka topic '%s'%n", trade, topicName);
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        bloom.start();

        Thread reuter = new Thread(() -> {
            String topicName = "reuter";

            String trade_names[] = {"microsoft", "tesla", "bhp", "vanguard", "blackrock"};
            Random t_name_random = new Random(0);
            Random price_random = new Random(1);

            try (KafkaProducer<Long, String> producer = new KafkaProducer<>(kafkaProps())) {
                for (long eventCount = 0; ; eventCount++) {

                    String trade_name = trade_names[t_name_random.nextInt(5)];
                    int price = price_random.nextInt(10)*100;
                    String time = DateTime.now().toString();

                    String trade = "reuter-"+trade_name+","+price+","+time;

                    producer.send(new ProducerRecord<>(topicName, eventCount, trade));
                    System.out.format("Published '%s' to Kafka topic '%s'%n", trade, topicName);
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        reuter.start();
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        return props;
    }
}
