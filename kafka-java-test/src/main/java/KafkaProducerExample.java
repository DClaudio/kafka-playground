import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.LongStream;

public class KafkaProducerExample {

    public static void main(String ...args){
        runProducer(5);
    }

    private static void runProducer(final int sendMessageCount) {
        final String KAFKA_SERVERS = "localhost:9092";
        final Producer<String, String> producer = createProducer(KAFKA_SERVERS);
        long time = System.currentTimeMillis();

        Consumer<ProducerRecord<String, String>> sendRecordToKafka = getProducerLongConsumerBiFunction(producer, time);

        LongStream.range(time, time + sendMessageCount)
                .mapToObj(KafkaProducerExample::buildRecordToSend)
                .forEach(sendRecordToKafka);

        producer.flush();
        producer.close();
    }

    private static void asyncRunProducer(final int sendMessageCount) {
        final String KAFKA_SERVERS = "localhost:9092";
        final Producer<String, String> producer = createProducer(KAFKA_SERVERS);
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        Consumer<ProducerRecord<String, String>> sendRecordToKafka = getProducerLongConsumerBiFunction(producer, time);

        LongStream.range(time, time + sendMessageCount)
                .mapToObj(KafkaProducerExample::buildRecordToSend)
                .forEach(record -> {
                    producer.send(record, (metadata, exception) -> {
                        long elapsedTime = System.currentTimeMillis() - time;
                        if (metadata != null) {
                            System.out.printf("sent record(key=%s value=%s) " +
                                            "meta(partition=%d, offset=%d) time=%d\n",
                                    record.key(), record.value(), metadata.partition(),
                                    metadata.offset(), elapsedTime);
                        } else {
                            exception.printStackTrace();
                        }
                        countDownLatch.countDown();
                    });
                    try {
                        countDownLatch.await(25, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

        producer.flush();
        producer.close();
    }





    private static Consumer<ProducerRecord<String, String>> getProducerLongConsumerBiFunction(Producer<String, String> kafkaProducer, long startTime) {
        return record -> sendRecordToKafka(kafkaProducer, startTime, record);
    }

    private static void sendRecordToKafka(Producer<String, String> producer, Long time, ProducerRecord<String, String> record) {
        try {
            RecordMetadata metadata = producer.send(record).get();
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static ProducerRecord<String, String> buildRecordToSend(Long value) {
        String KAFKA_TOPIC = "test";
        return new ProducerRecord<>(KAFKA_TOPIC, value.toString(), "message number " + value);
    }

    private static Producer<String, String> createProducer(String kafkaServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-java-producer-example");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        return new KafkaProducer<>(props);
    }
}
