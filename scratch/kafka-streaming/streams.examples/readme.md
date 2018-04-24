Apache Kafka and Kafka streams tutorials based on:

- [Tutorial on Apache streams website](https://kafka.apache.org/11/documentation/streams/tutorial)
- [AWS Java SDK Sample repo](https://github.com/aws-samples/aws-java-sample)

The code at `myapps/ImageInfoConsumer.java` reads from a topic where S3 keys for an image are published, and writes some information about the image (shape and mean pixel intensity) to an output topic.
