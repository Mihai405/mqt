package hw11;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class AggregateTopics {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID = "aggregateTopics";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put("application.id", "aggregateTopics-application");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> first_stream = builder.stream("first-topic-stream");

        KStream<String, String> second_stream = builder.stream("second-topic-stream");

        KStream<String, String> aggregatedStream = first_stream.merge(second_stream);

        aggregatedStream.to("aggregated-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
