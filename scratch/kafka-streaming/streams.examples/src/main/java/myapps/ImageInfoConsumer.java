/**
 * Input: Key for of an image stored in S3 bucket.
 * Processing:
 *  - Download image from S3.
 *  - Load image into an ND-array (without writing to disk).
 *  - Compute shape.
 *  - Compute mean pixel intensity.
 * Output: String containing (Image key, Image shape, Mean pixel intensity).
 */

package myapps;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.transferlearning.TransferLearning;
import org.deeplearning4j.zoo.PretrainedType;
import org.deeplearning4j.zoo.ZooModel;
import org.deeplearning4j.zoo.model.ResNet50;
import org.nd4j.linalg.api.ndarray.INDArray;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ImageInfoConsumer {

    public static void main(String[] args) throws Exception {

        // Configuration.
        final String bootstrapServer = "localhost:9092";
        final String appID = "streams-image-info-consumer";
        final String inputTopic = "streams-plaintext-input";
        final String outputTopic = "streams-image-info-output";
        final String bucketName = "klibisz-twitter-stream";
        final String awsRegion = "us-east-1";

        // Map specifying the stream execution configuration.
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        // Specify serialization and deserialization libraries.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // S3 client used to download images.
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);

        // Convnet setup.
        final NativeImageLoader imageLoader = new NativeImageLoader(224, 224, 3, true);
        final ZooModel zooModel = new ResNet50();
        final ComputationGraph fullConvNet = (ComputationGraph) zooModel.initPretrained(PretrainedType.IMAGENET);
        final ComputationGraph truncatedConvNet = new TransferLearning.GraphBuilder(fullConvNet)
                .removeVertexAndConnections("fc1000").setOutputs("flatten_3").build();

        // Define computational logic of the streams application as a topology of nodes.
        final StreamsBuilder builder = new StreamsBuilder();

        // Create source stream from specific Kafka topic containing key-value pairs.
        KStream<String, String> imageKeys = builder.stream(inputTopic);

        KStream<String, String> imageInfos = imageKeys.mapValues((imageKey) -> {

            // Download image from S3 bucket into memory.
            System.out.println(String.format("Downloading %s from S3", imageKey));
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, imageKey));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());

            String imageInfo = "No image information available";
            String vectorInfo = "No vector information available";

            // Read image into an n-dimensional array.
            try {
                INDArray image = imageLoader.asMatrix(object.getObjectContent());
                int[] shape = image.shape();
                imageInfo = String.format(
                    "Key = %s, shape = (%d x %d x %d), mean intensity = %.3f",
                    imageKey, shape[2], shape[3], shape[1], image.meanNumber());
                System.out.println("Image info: " + imageInfo);

                INDArray featureVector = truncatedConvNet.outputSingle(image);
                vectorInfo = String.format("" +
                        "Shape = %s, min = %.3f, mean = %.3f, max = %.3f",
                        featureVector.shapeInfoToString(), featureVector.minNumber(),
                        featureVector.meanNumber(), featureVector.maxNumber());
                System.out.println("Vector info: " + vectorInfo);

            } catch(IOException ex) {
                System.out.println("Problem reading image " + ex);
            }

            return imageInfo;
        });

        // Write the image information to output topic.
        imageInfos.to(outputTopic);

        // Finalize and describe topology.
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
