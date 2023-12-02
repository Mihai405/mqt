package hw3;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class StringProducer {
    private static final String OUR_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OUR_CLIENT_ID = "stringProducer";
    private static Producer<String, String> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, OUR_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, OUR_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> data = new ProducerRecord<>("topic1", "key", "value1");
        ProducerRecord<String, String> data1 = new ProducerRecord<>("topic2",  "value2");
        try {
            producer.send(data).get();
            producer.send(data1).get();
        }catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
