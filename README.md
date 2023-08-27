# BDSPRO_KafkaCEP
This project is an example of using Kafka Stream CEP library. Goal of the project is to compare the event time latency with Apache Flink and Apache Flink CEP. The code was written using IntelliJ IDEA 2022.1 Ultimate Edition and tested on Mac OS using Apache Kafka 3.5.0.  

# Architecture
![alt text](https://github.com/Gerti10/BDPRO_KafkaCEP/blob/d9e3da797eaf6bdbd1bdd36112ab4b633a6a12be/doc/KafkaCEP_Architecture.png?raw=true)

The architecture of the project is as follows. The class TopicProducer creates two input topics (Velocities, Quantities) and one output topic (Matches) where the matched events as well as the the latency for each pattern is saved. In this class we can also specify the number of partitions for the topics as well as the replication factor which by default are both 1. Increasing the partition number enables parallel processing when publishing the events and replication factor enables resilience when cluster nodes become unavailable.

The class CSVProducer takes as arguments the path of the csv file and the throughput threshold. It reads then data from the CSV file, serializes the data in JSON format using JSONSerializer class and creates general events which contain both the velocity and quantity attributes. Using Kafka producers the events are send to the corresponding topics and are ready to be processed by the CEP library. It also keeps track of the throughput and ensures then the throughput will not be larger than specified in the program arguments. Note that velocity events will always have attribute quantity 0.0 and the quantity events will always have attribute velocity 0.0. The reason is to use the same serialization/deserialization classes for both event types in the CEP. This is a workaround to be able to create sequence patterns from two different event types, because the CEP library does not offer this functionality.

For each of the Patterns defined in the Pattern class there is a main class with the same name as the pattern which defines the stream configuration of Kafka,  topology used in Kafka Streams and the output JSON that is written in the output topic. The stream is configured to use the VelocityEventSerde class to serialize the JSON data into byte array and deserialize them again. Such configuration is mandatory for Kafka Streams.

To extract the event time latency of each pattern the class LogConsumer is used. It implements a Kafka consumer which subscribes to the CEP output topic and extracts the latency measurement as well the the time when the pattern is created. In order to reduce the large amount of logs it will sum all latencies for each pattern found in one second as well as count the patterns found and it will save the logs on the project root directory under the name _log_output_.

# How to Run

1. First download and extract Apache Kafka from the official website [Apache Kafka](https://kafka.apache.org/downloads)
2. Install Multirun plugin to be able to run classes simultaneously [Multirun Plugin](https://plugins.jetbrains.com/plugin/7248-multirun)
3. Clone the Project in IntelliJ - The implementation of Kafka Stream CEP is in example package. (core and streams packages are the CEP library)
5. Using Edit Configuration, add the program arguments for the CSVProducer class (CSV file and throughput)
6. Using terminal switch to Apache Kafka folder and run the Zookeeper using: **bin/zookeeper-server-start.sh config/zookeeper.properties**
7. In a new terminal run Kafka server using: **bin/kafka-server-start.sh config/server.properties**
8. Build and Run TopicProducer using TopicProducer.run.xml cofiguration file [Topic Producer Configuration](https://github.com/Gerti10/BDPRO_KafkaCEP/blob/main/.run/TopicProducer.run.xml)
9. Run simultaneously CSVProducer and the class that runs the desired pattern(Ex. Q6_Iter to run with the Query 6 on the pattern catalogue) using the run configuration files [CSVProducer and Q6_Iter Configuration](https://github.com/Gerti10/BDPRO_KafkaCEP/blob/main/.run/Q6_Iter_Compound.run.xml)
10. When pattern matching is finished run the LogConsumer to create the logs using the run configuration file [Log Consumer Configuration](https://github.com/Gerti10/BDPRO_KafkaCEP/blob/main/.run/LogsConsumer.run.xml)
