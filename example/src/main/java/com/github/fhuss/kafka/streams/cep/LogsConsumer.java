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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class LogsConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(LogsConsumer.class);
    public static void main(String[] args) throws IOException {
        long lastLogTimeMs = -1;
        long totalLatencySum = 0;
        int matchedPatternsCount = 0;
        BufferedWriter writter = new BufferedWriter(new FileWriter("log_output"));
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Log-Reader");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try(KafkaConsumer<String, String> logConsumer = new KafkaConsumer<String, String>(props)){
            logConsumer.subscribe(List.of("Matches"));
            while(true){
                ConsumerRecords<String, String> records = logConsumer.poll(Duration.ofMillis(10000));

                for (ConsumerRecord<String, String> record : records){
                    if (record.key() ==null){
                        continue;
                    }
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(record.value());
                    JSONArray jsonArray = (JSONArray) json.get("latencies");
                    JSONObject arrayElement = (JSONObject)jsonArray.get(1);
                    if(arrayElement.isEmpty()) arrayElement = (JSONObject) jsonArray.get(2);//For patterns with 3 stages

                    //initialize
                    if (lastLogTimeMs == -1){
                        lastLogTimeMs = (long) arrayElement.get("currenttime");
                        matchedPatternsCount++;
                        LOG.info("starting latency logging for matching patterns with frequency 1 second");
                    }else if ((long) arrayElement.get("currenttime") - lastLogTimeMs < 1000){
                        //when the difference of the timestamp is more than 1 second start a new row
                        totalLatencySum += (long) arrayElement.get("currenttime");
                        matchedPatternsCount ++;
                    }else {
                        totalLatencySum += (long) arrayElement.get("currenttime");
                        matchedPatternsCount ++;
                        String log = "totalLatencySum: $" + totalLatencySum + "$ matchedPatternsSum: $"
                                + matchedPatternsCount + "$";
                        writter.write(log + System.lineSeparator());
                        writter.flush();
                        lastLogTimeMs = -1;
                        totalLatencySum = 0;
                        matchedPatternsCount = 0;
                    }
                }
            }
        }catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
