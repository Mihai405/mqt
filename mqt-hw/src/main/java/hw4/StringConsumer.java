package hw4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class StringConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(StringConsumer.class);
    private static final String OUR_BOOTSTRAP_SERVERS = ":9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String CONSUMER_GROUP_ID = "group_1";

    private final KafkaConsumer<String, String> kafkaConsumer;

    public StringConsumer(Properties consumerProperties){
        kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
    }

    public static Properties buildConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OUR_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    public void pollKafka(String kafkaTopicName){

        kafkaConsumer.subscribe(Collections.singleton(kafkaTopicName));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(pollingTime);
            records.forEach(crtRecord -> {
                LOG.info("String consumer: topic ={}  key = {}, value = {} => partition = {}, offset = {}", kafkaTopicName, crtRecord.key(), crtRecord.value(), crtRecord.partition(), crtRecord.offset());
            });
        }
    }

    public static void main(String[] args) {
        StringConsumer consumer = new StringConsumer(buildConsumerProperties());
        consumer.pollKafka("topic1");
    }
}
