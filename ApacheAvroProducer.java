package data_generator_simulator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Files;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvBadConverterException;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class ApacheAvroProducer {
    private static Logger log = LoggerFactory.getLogger(ApacheAvroProducer.class.getName());

    public static void main(String[] args) {
        String KAFKA_TOPIC = SampleUtils.setStringParameters("KAFKA_TOPIC", "ce_online_retail_II");
        int TOTAL_RECORDS = SampleUtils.setIntegerParameters("TOTAL_RECORDS", 100);
        String BOOTSTRAP_SERVERS = SampleUtils.setStringParameters("BOOTSTRAP_SERVERS", "kafka:9092");
        String SCHEMA_REGISTRY_URL = SampleUtils.setStringParameters("SCHEMA_REGISTRY_URL",
                "http://schema-registry:8081");
        String SCHEMA_PATH = SampleUtils.setStringParameters("SCHEMA_PATH",
                "/input_data/online_retail_II_serialize_schema.json");
        String CSV_FILE = SampleUtils.setStringParameters("CSV_FILE", "/input_data//online_retail_II.csv");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        log.info("Topic Name: {}, Total Records: {}, Bootstrap Servers: {}, Schema Registry URL: {}",
                KAFKA_TOPIC, TOTAL_RECORDS, BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);

        final KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "Shutdown-thread"));

        // load schema from file and register to schema_register
        String valueSchemaString = "";
        try {
            valueSchemaString = new String(Files.readAllBytes(Paths.get(SCHEMA_PATH)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Schema avroValueSchema = new Schema.Parser().parse(valueSchemaString);
        // query the latest schema
        RestService rest_service = new RestService(SCHEMA_REGISTRY_URL);

        String subject_name = KAFKA_TOPIC + "-value";
        String old_schema_str = "";
        try {
            old_schema_str = rest_service.getLatestVersion(subject_name).getSchema();
        } catch (IOException ex) {
            ex.printStackTrace();
            log.error("can not connect with schema registry:" + SCHEMA_REGISTRY_URL);
            System.exit(1);
        } catch (RestClientException ex) {
            log.info("can not find the schema with subject <" +  subject_name +"> and register");
        }

        try {
            String new_schema_str = avroValueSchema.toString();
            if (old_schema_str.compareTo(new_schema_str) != 0) {
                rest_service.registerSchema(new_schema_str, subject_name);
            }
        } catch (IOException | RestClientException ex) {
            ex.printStackTrace();
            log.error("meet error when registering <" + subject_name + "> schema to schema registry:" + SCHEMA_REGISTRY_URL);
            System.exit(1);
        }

        AtomicLong errorCount = new AtomicLong();
        CountDownLatch requestLatch = new CountDownLatch(TOTAL_RECORDS);

        final AtomicLong successCount = new AtomicLong();

        Callback postSender = (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error adding to topic", e);
                errorCount.incrementAndGet();
            } else {
                successCount.incrementAndGet();
            }
            requestLatch.countDown();
        };

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(
                () -> log.info("Successfully created {} Kafka records", successCount.get()),
                2, SampleUtils.PROGRESS_REPORTING_INTERVAL, TimeUnit.SECONDS);

        int total_read_lines = 0;
        try (Reader file_reader = Files.newBufferedReader(Paths.get(CSV_FILE));
                CSVReader csv_reader = new CSVReaderBuilder(file_reader).withSkipLines(1).build()) {
            String[] record;
            while (total_read_lines < TOTAL_RECORDS && (record = csv_reader.readNext()) != null) {
                GenericRecord thisValueRecord = new GenericData.Record(avroValueSchema);
                thisValueRecord.put("Invoice", record[0]);
                thisValueRecord.put("StockCode", record[1]);
                thisValueRecord.put("Description", record[2]);
                thisValueRecord.put("Quantity", Integer.parseInt(record[3]));
                thisValueRecord.put("InvoiceDate", record[4]);
                thisValueRecord.put("Price", Float.parseFloat(record[5]));
                thisValueRecord.put("CustomerID", record[6]);
                thisValueRecord.put("Country", record[7]);
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>(KAFKA_TOPIC,
                        Integer.toString(total_read_lines),
                        datumToByteArray(avroValueSchema, thisValueRecord));
                producer.send(producerRecord, postSender);
                total_read_lines++;
            }

            // Wait for sends to complete.
            requestLatch.await();

        } catch (IOException | CsvBadConverterException | InterruptedException ex) {
            ex.printStackTrace();
        }

        // Stop the thread that periodically reports progress.
        scheduler.shutdown();
        // shutdown producer
        producer.close();
    }

    public static byte[] datumToByteArray(Schema schema, GenericRecord datum) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(datum, e);
            e.flush();
            byte[] byteData = os.toByteArray();
            return byteData;
        } finally {
            os.close();
        }
    }
}
