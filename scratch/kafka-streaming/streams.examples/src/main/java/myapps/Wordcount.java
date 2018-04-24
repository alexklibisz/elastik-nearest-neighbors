package myapps;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Wordcount {
    public static void main(String[] args) throws Exception {

        // Map specifying the stream execution configuration.
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Specify serialization and deserialization libraries.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Define computational logic of the streams application as a topology of nodes.
        final StreamsBuilder builder = new StreamsBuilder();

        // Create source stream from specific Kafka topic containing key-value pairs.
        KStream<String, String> source = builder.stream("streams-plaintext-input");

        // Apply a flatmap that just splits each line into its constituent words.
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")));

        // To do counting aggregation, you have to group items by a key to maintain their state.
        // In this case, the key is just the value, which is the word from the flatmap above.
        // The groupBy is followed by a count operation which stores its state in the "counts-store".
        KTable<String, Long> counts = words.groupBy((key, value) -> value).count();

        // Specify the output location and format.
        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Create and describe topology.
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // Define the stream.
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Define shutdown handler with a countdown.
        final CountDownLatch latch = new CountDownLatch(3);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // Start running.
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
