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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;

public class VelocityEventSerde implements Serde<GeneralEvent> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<GeneralEvent> serializer() {
        return new JsonSerDeserializer();
    }

    @Override
    public Deserializer<GeneralEvent> deserializer() {
        return new JsonSerDeserializer();
    }

    public static class JsonSerDeserializer implements Serializer<GeneralEvent>, Deserializer<GeneralEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        @SuppressWarnings("unchecked")
        public GeneralEvent deserialize(String topic, byte[] data) {
            if( data == null ) return null;
            JSONParser parser = new JSONParser();

            try {
                JSONObject jsonObject = (JSONObject) parser.parse(new String(data, "UTF-8"));
                return new GeneralEvent(
                        (String)jsonObject.get("sensorid"),
                        (Double)jsonObject.get("longitude"),
                        (Double)jsonObject.get("latitude"),
                        (Long)jsonObject.get("timestamp"),
                        (Long)jsonObject.get("systemtimestamp"),
                        (Double)jsonObject.get("velocity"),
                        (Double)jsonObject.get("quantity")
                );
            } catch (ParseException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] serialize(String topic, GeneralEvent data) {
            if( data == null ) return null;
            JSONObject json = new JSONObject();
            json.put("sensorid", data.sensorid);
            json.put("longitude", data.longitude);
            json.put("latitude", data.latitude);
            json.put("timestamp", data.timestamp);
            json.put("systemtimestamp", data.systemtimestamp);
            json.put("velocity", data.velocity);
            json.put("quantity", data.quantity);

            return json.toJSONString().getBytes(Charset.forName("UTF-8"));
        }

        @Override
        public void close() {

        }
    }

}
