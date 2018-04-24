package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {
    public static void main(String[] args) throws Exception {

        // Map specifying the stream execution configuration.
        Properties props = new Properties();

        // Identify this application vs. others talking to Kafka.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");

        // Specify host/port to establish connection to local Kafka instance.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Specify serialization and deserialization libraries.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Define computational logic of the streams application as a topology of nodes.
        final StreamsBuilder builder = new StreamsBuilder();

        // Create source stream from specific Kafka topic containing key-value pairs.
        KStream<String, String> source = builder.stream("streams-plaintext-input");

        // Just write the source to another Kafka topic.
        source.to("streams-pipe-output");

        // Finalize the topology.
        final Topology topology = builder.build();

        // Print description of the topology.
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
