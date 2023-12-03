package hw7;

import avro.mqt.streams.OrderAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderProducerAvro {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "orderAvroProducer";
    private static Producer<String, OrderAvro> producer;

    public static Properties buildConsumerProperties(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");
        return props;
    }

    public static void main(String[] args){
        producer = new KafkaProducer<>(buildConsumerProperties());
        OrderAvro orderAvro = new OrderAvro("order1", "client1", "10");
        ProducerRecord<String, OrderAvro> data1 = new ProducerRecord<>("topic1", orderAvro);
        try {
            producer.send(data1).get();
        } catch (InterruptedException | ExecutionException e){
            producer.flush();
        }
        producer.close();
    }
}
