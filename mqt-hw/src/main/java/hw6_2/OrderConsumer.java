package hw6_2;

import hw5_2.Order;
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

public class OrderConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(OrderConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String CONSUMER_GROUP_ID = "orderConsumer";

    private static KafkaConsumer<String, Order> kafkaConsumer;

    public OrderConsumer(Properties consumerPropsMap){
        kafkaConsumer = new KafkaConsumer<String, Order>(consumerPropsMap);
    }

    public static Properties buildConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
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
            ConsumerRecords<String, Order> records = kafkaConsumer.poll(pollingTime);

            records.forEach(record -> {
                LOG.info("JSON Consumer: topic = {} value = {}", kafkaTopicName,  record.value());
            });
        }
    }

    public static void main(String[] args) {
        OrderConsumer consumer = new OrderConsumer(buildConsumerProperties());
        consumer.pollKafka("topic1");
    }
}
