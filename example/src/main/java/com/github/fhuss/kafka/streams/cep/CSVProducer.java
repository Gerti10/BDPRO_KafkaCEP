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

import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CSVProducer {

    private static double throughput = 0;
    private static final Logger LOG = LoggerFactory.getLogger(CSVProducer.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String sourceFile = "example/src/main/resources/QnViter10.csv";

        ReadCSV readCSV;
        List dataPoints = null;

        if (args.length != 0){
             readCSV = new ReadCSV(args[0]);
             dataPoints = readCSV.ReadCSVFile();
             throughput = Double.parseDouble(args[1]);
        }else {
            System.err.println("arguments missing to main method");
        }


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        LOG.info("Creating General Event Producer");
        KafkaProducer<String, GeneralEvent> generalEventProducer = new KafkaProducer<>(props);
        LOG.info("General Event Producer created");
        LOG.info("Generating the events");
        for (int i = 0; i < 5000; i++){
            for (Object dataPoint : dataPoints){
                QnVEvent qnVEvent = (QnVEvent) dataPoint;
                qnVEvent.point = qnVEvent.point.replace("(", " ");
                qnVEvent.point = qnVEvent.point.replace(")", " ");
                String[] points = qnVEvent.point.split(" ");
                ProducerRecord<String, GeneralEvent> velocityRecord = new ProducerRecord<>(
                        "velocities",
                        "key1",
                        new GeneralEvent(qnVEvent.sensorid,
                                Double.parseDouble(points[2]),
                                Double.parseDouble(points[3]),
                                qnVEvent.timestamp,
                                System.currentTimeMillis(),
                                qnVEvent.velocity,
                                0));


                generalEventProducer.send(velocityRecord);

                ProducerRecord<String, GeneralEvent> quantityRecord = new ProducerRecord<>(
                        "quantities",
                        "key1",
                        new GeneralEvent(
                                qnVEvent.sensorid,
                                Double.parseDouble(points[2]),
                                Double.parseDouble(points[3]),
                                qnVEvent.timestamp,
                                System.currentTimeMillis(),
                                0,
                                qnVEvent.quantity));

                Future<RecordMetadata> recordMetadata = generalEventProducer.send(quantityRecord);
                int serializedRecordLength = recordMetadata.get().serializedKeySize() + recordMetadata.get().serializedValueSize();

                Map<MetricName, ? extends Metric> metrics = generalEventProducer.metrics();
                metrics.values().forEach(v -> {
                    if (Objects.equals(v.metricName().name(), "outgoing-byte-rate")){
                        double outgoingByteRate = (double) v.metricValue();
                        //Log.info("recordLength: " + serializedRecordLength);

                        //serialized recordLength is 163 Bytes
                        //outgoing_byte_rate is the average number of outgoing bytes per second
                        Log.info("outgoing_byte_rate: " + outgoingByteRate);
                        double tuplesPerSecond = outgoingByteRate/serializedRecordLength;
                        Log.info("tuples per second: " + tuplesPerSecond);
                        if (tuplesPerSecond > throughput){
                            try {
                                LOG.info("Thread sleeping");
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                });
            }
        }
        //TODO: Get throughput of publishing messages to topic
        /*LOG.info("Reading Metrics");
        Map<MetricName, ? extends Metric> metrics = generalEventProducer.metrics();
        metrics.values().forEach(v ->
                //LOG.info("MetricName: " + v.metricName().name() + " Value: " + v.metricValue().toString())
        );*/

        LOG.info("Finished Generating Events");
        LOG.info("Closing General Event Producer");
        generalEventProducer.close();
    }
}
