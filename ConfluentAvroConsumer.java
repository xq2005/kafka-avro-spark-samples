package data_generator_simulator;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;


public class ConfluentAvroConsumer {
    private static Logger log = LoggerFactory.getLogger(ConfluentAvroConsumer.class.getName());

    public static void main(String[] args) {
        String KAFKA_TOPIC = SampleUtils.setStringParameters("KAFKA_TOPIC", "ce_online_retail_II");
        int TOTAL_RECORDS = SampleUtils.setIntegerParameters("TOTAL_RECORDS", 100);
        String BOOTSTRAP_SERVERS = SampleUtils.setStringParameters("BOOTSTRAP_SERVERS", "kafka:9092");
        String SCHEMA_REGISTRY_URL = SampleUtils.setStringParameters("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        log.info("Topic Name: {}, Total Records: {}, Bootstrap Servers: {}, Schema Registry URL: {}",
                KAFKA_TOPIC, TOTAL_RECORDS, BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList(KAFKA_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(),
                            record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
