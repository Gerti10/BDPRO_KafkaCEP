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


public class GeneralEvent {
    
    public String sensorid;
    public double longitude;
    public double latitude;
    public long timestamp;
    public long systemtimestamp;
    public double velocity;
    public double quantity;

    public GeneralEvent(){}
    public GeneralEvent(String sensorid, double longitude, double latitude, long timestamp, long systemtimestamp,
                        double velocity, double quantity) {
        this.sensorid = sensorid;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.systemtimestamp = systemtimestamp;
        this.velocity = velocity;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VelocityEvent{");
        sb.append("sensorid='").append(sensorid).append('\'');
        sb.append(", longitude=").append(longitude);
        sb.append(", latitude=").append(latitude);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", velocity=").append(velocity);
        sb.append(", quantity=").append(quantity);
        sb.append('}');
        return sb.toString();
    }
}
