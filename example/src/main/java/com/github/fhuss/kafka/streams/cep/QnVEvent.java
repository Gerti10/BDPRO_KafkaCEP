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

import com.opencsv.bean.CsvBindByPosition;

public class QnVEvent {
    @CsvBindByPosition(position = 0)
    public String sensorid;
    @CsvBindByPosition(position = 1)
    public String point;
    @CsvBindByPosition(position = 2)
    public long timestamp;
    @CsvBindByPosition(position = 3)
    public double velocity;
    @CsvBindByPosition(position = 4)
    public double quantity;
    @CsvBindByPosition(position = 5)
    public String rest;


    public String toString(){
        final StringBuilder sb = new StringBuilder("QnVEvent{");
        sb.append("sensorid='").append(sensorid).append('\'');
        sb.append(", point=").append(point);
        sb.append(", timestamp=").append(timestamp);
        sb.append(", velocity=").append(velocity);
        sb.append(", quantity=").append(quantity);
        sb.append('}');
        return sb.toString();
    }

    public String getSensorId(){
        return sensorid;
    }

    public String getPoint(){
        return point;
    }
    public double getVelocity() {
        return velocity;
    }

    public double getQuanty() {
        return quantity;
    }

    public String getRest() {
        return rest;
    }

    public long getTimeStamp() {return timestamp; }
}
