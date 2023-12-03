package hw5;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class JSONProducerApi {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "jsonProducer";
    private static Producer<String, String> producer;

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        String json="{\n" +
                "    \"comanda\":\"order1\",\n" +
                "    \"client_id\":\"client1\",\n" +
                "    \"numar_produse\":\"10\"\n" +
                "}";

        ProducerRecord<String, String> data1 = new ProducerRecord<>("topic1", json);
        try {
            producer.send(data1).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
