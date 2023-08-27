/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep;

import com.github.fhuss.kafka.streams.cep.core.Sequence;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;





public class CEPStockDemo {

    private static final Logger LOG = LoggerFactory.getLogger(CEPStockDemo.class);

    public static void main(String[] args) {

        LOG.info("Starting Stocks Application");

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stocks-demo");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VelocityEventSerde.class);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200000000);
        streamsConfiguration.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 200000000);


        List<String> inputTopics = new ArrayList<>();
        inputTopics.add("velocities");
        inputTopics.add("quantities");
        Topology topology2 = CEPStockDemo.topology2("velocities", inputTopics, "Matches");
        //Topology topology = CEPStockDemo.topology("velocities", "velocities", "Matches");

        KafkaStreams kafkaStreams = new KafkaStreams(topology2, streamsConfiguration);
        kafkaStreams.cleanUp();

        kafkaStreams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping Stocks Application");
            kafkaStreams.close();
        }));
    }


    public static Topology topology2(final String queryName, List<String> inputTopics, final String outputTopic){

        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        try (VelocityEventSerde velocityEventSerde = new VelocityEventSerde()){

            CEPStream<String, GeneralEvent> stream = builder.stream(inputTopics,
                    Consumed.with(Serdes.String(), Serdes.serdeFrom(velocityEventSerde.serializer(),
                            velocityEventSerde.deserializer())));

            KStream<String, Sequence<String, GeneralEvent>> events = stream.query(queryName,
                    Patterns.SEQ_2_SKIP_TIL_NEXT_MATCH);

            events.mapValues(CEPStockDemo::sequenceAsJson)
                    .through(outputTopic, Produced.with(null, Serdes.String()))
                    .print(Printed.toSysOut());
        }
        return builder.build();
    }
    public static Topology topology(final String queryName,
                                    final String inputTopic,
                                    final String outputTopic) {
        // Build query
        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, GeneralEvent> stream = builder.stream(inputTopic);
        KStream<String, Sequence<String, GeneralEvent>> velocities = stream.query(queryName, Patterns.Q7ITER);

        velocities.mapValues(seq -> sequenceAsJson(seq))
                .through(outputTopic, Produced.with(null, Serdes.String()))
                .print(Printed.toSysOut());

        return builder.build();
    }

    private static String sequenceAsJson(Sequence<String, GeneralEvent> seq) {
        JSONObject json = new JSONObject();
        JSONArray events = new JSONArray();
        json.put("events", events);
        seq.matched().forEach( v -> {
            JSONObject stage = new JSONObject();
            stage.put("name", v.getStage());
            stage.put("events", v.getEvents().stream().map(e -> e.value().sensorid).collect(Collectors.toList()));
            stage.put("velocities", v.getEvents().stream().map(e -> e.value().velocity).collect(Collectors.toList()));
            stage.put("quantities", v.getEvents().stream().map(e -> e.value().quantity).collect(Collectors.toList()));
            stage.put("starttimestamp", v.getEvents().stream()
                    .map(e -> e.value().systemtimestamp).collect(Collectors.toList()));
            stage.put("endtimestamp", System.currentTimeMillis());
            stage.put("Lattency", v.getEvents().stream()
                    .map(e -> System.currentTimeMillis() - e.value().systemtimestamp).collect(Collectors.toList()));
            events.add(stage);
        });
        return json.toJSONString();
    }
}
