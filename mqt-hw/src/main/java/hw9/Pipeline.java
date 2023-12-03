package hw9;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "pipelineProducer";
    private static final String OFFSET_RESET = "earliest";
    private static final String CONSUMER_GROUP_ID = "pipelineConsumer";

    private static KafkaConsumer<String, String> kafkaConsumer;
    private static Producer<String, String> kafkaProducer;

    public Pipeline() {
        kafkaConsumer = new KafkaConsumer<String, String>(buildConsumerProperties());
        kafkaProducer = new KafkaProducer<String, String>(buildProducerProperties());
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
    public static Properties buildProducerProperties(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public void executePipeline(String sourceTopic, String targetTopic){

        kafkaConsumer.subscribe(java.util.Collections.singleton(sourceTopic));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);
        while(true){
            kafkaConsumer.poll(pollingTime)
                .forEach(record -> {
                    LOG.info("JSON Consumer: topic = {} value = {}", sourceTopic,  record.value());
                    ProducerRecord<String, String> data = new ProducerRecord<>(targetTopic, record.value());
                    try {
                        kafkaProducer.send(data).get();
                    } catch (InterruptedException | java.util.concurrent.ExecutionException e){
                        kafkaProducer.flush();
                    }
                });
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = new Pipeline();
        pipeline.executePipeline("topic1", "topic2");
    }
}
