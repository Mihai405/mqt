package hw11;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicsProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "topicsProducer";
    private static Producer<String, String> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> data1 = new ProducerRecord<>("first-topic-stream", "value1");
        ProducerRecord<String, String> data2 = new ProducerRecord<>("second-topic-stream",  "value2");
        try {
            producer.send(data1).get();
            producer.send(data2).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
