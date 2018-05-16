import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static java.util.Collections.singletonList;

public class KafkaConsumerExample {

    public static void main(String... args) throws InterruptedException {
        System.out.println("do stuff");
        runConsumer();
    }


    private static void runConsumer() throws InterruptedException {
        Consumer<String, String> kafkaConsumer = createConsumer();

        final int giveUp = 10000;
        int noRecordsCount = 0;

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            kafkaConsumer.commitAsync();
            Thread.sleep(1000);
        }
        kafkaConsumer.close();
        System.out.println("DONE");
    }


    private static Consumer<String, String> createConsumer() {
        final String KAFKA_SERVERS = "localhost:9092";
        final String TOPIC = "test";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        props.put(ConsumerConfig.ACKS_CONFIG, "all");
//        props.put(ConsumerConfig.RETRIES_CONFIG, 0);
//        props.put(ConsumerConfig.BATCH_SIZE_CONFIG, 16384);
//        props.put(ConsumerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ConsumerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(singletonList(TOPIC));

        return kafkaConsumer;
    }
}
