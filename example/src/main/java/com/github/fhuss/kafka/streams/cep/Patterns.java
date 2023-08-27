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

import com.github.fhuss.kafka.streams.cep.core.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.core.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.core.pattern.Selected;

import java.util.concurrent.TimeUnit;

public class Patterns {

    //Iteration Patterns
    public static final Pattern<String, GeneralEvent> Q6ITER = new QueryBuilder<String, GeneralEvent>()
            .select("stage-1")
            .where(event -> event.value().velocity > 0)
            .<Double>fold("velocity", (k, v, curr) -> v.velocity)
            .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
            .then()
            .select("stage-2", Selected.withSkipTilNextMatch())
            .times(4)
            .where((event, state) -> (Double)state.get("velocity") < event.value().velocity)
            .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .<Double>fold("velocity", (k, v, curr) -> v.velocity)
            .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)

            .within(1, TimeUnit.HOURS)
            .build();
    public static final Pattern<String, GeneralEvent> Q7ITER = new QueryBuilder<String, GeneralEvent>()
            .select("stage-1")
            .where((event, states) -> event.value().velocity > 175 )
            .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
            .then()
            .select("stage-2", Selected.withSkipTilNextMatch())
            .times(4)
            .where((event, states) -> event.value().velocity > 175)
            .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .within(1, TimeUnit.HOURS)
            .build();

    //Sequence 1 Patterns
    public static final Pattern<String,GeneralEvent> SEQ_1_SKIP_TIL_NEXT_MATCH =
            new QueryBuilder<String,GeneralEvent>()
            .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                .where((event, states) -> event.value().velocity > 175.0)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                    .<GeneralEvent>fold("event1", (k, v, curr) -> v)
                .then()
            .select("stage-2", Selected.withSkipTilNextMatch().withTopic("quantities"))
                .where((event, states) -> event.value().quantity > 175.0)
                .and((event, states) -> checkDistance(event.value(), (GeneralEvent) states.get("event1")) < 100)
                .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .within(1, TimeUnit.HOURS)
            .build();

    public static final Pattern<String,GeneralEvent> SEQ_1_SKIP_TIL_ANY_MATCH =
            new QueryBuilder<String,GeneralEvent>()
            .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                .where((event, states) -> event.value().velocity > 175.0)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                    .<GeneralEvent>fold("event1", (k, v, curr) -> v)
                .then()
            .select("stage-2", Selected.withSkipTilAnyMatch().withTopic("quantities"))
                .where((event, states) -> event.value().quantity > 175.0)
                .and((event, states) -> checkDistance(event.value(), (GeneralEvent) states.get("event1")) < 100)
                .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .within(1, TimeUnit.HOURS)
            .build();

    public static final Pattern<String, GeneralEvent> SEQ_1_WITH_STRICT_CONTIGUITY =
            new QueryBuilder<String, GeneralEvent>()
            .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                .where((event, states) -> event.value().velocity > 175.0)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                    .<GeneralEvent>fold("event1", (k, v, curr) -> v)
                .then()
            .select("stage-2", Selected.withStrictContiguity().withTopic("quantities"))
                .where((event, states) -> event.value().quantity > 175.0)
                .and((event, states) -> checkDistance(event.value(), (GeneralEvent) states.get("event1")) < 100)
                .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .within(1, TimeUnit.HOURS)
            .build();

    //Sequence 2 Patterns
    public static final Pattern<String, GeneralEvent> SEQ_2_SKIP_TIL_NEXT_MATCH =
            new QueryBuilder<String, GeneralEvent>()
            .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                .where((event, states) -> event.value().velocity > 175.0)
                    .<Double>fold("velocity1", (k, v, curr) -> v.velocity)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                .then()
            .select("stage-2", Selected.withSkipTilNextMatch().withTopic("velocities"))
                .where((event, states) -> event.value().velocity > 175.0)
                .and((event, states) -> event.value().velocity >= (Double) states.get("velocity1"))
                .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
                .then()
            .select("stage-3", Selected.withSkipTilNextMatch().withTopic("quantities"))
                .where((event, states) -> event.value().quantity < 200)
                .and((event, states) -> event.value().quantity < (Double) states.get("velocity1"))
                .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
            .within(1, TimeUnit.HOURS)
            .build();

    public static final Pattern<String, GeneralEvent> SEQ_2_SKIP_TIL_ANY_MATCH =
            new QueryBuilder<String, GeneralEvent>()
                    .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                    .where((event, states) -> event.value().velocity > 175.0)
                    .<Double>fold("velocity1", (k, v, curr) -> v.velocity)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                    .then()
                    .select("stage-2", Selected.withSkipTilAnyMatch().withTopic("velocities"))
                    .where((event, states) -> event.value().velocity > 175.0)
                    .and((event, states) -> event.value().velocity >= (Double) states.get("velocity1"))
                    .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
                    .then()
                    .select("stage-3", Selected.withSkipTilAnyMatch().withTopic("quantities"))
                    .where((event, states) -> event.value().quantity < 200)
                    .and((event, states) -> event.value().quantity < (Double) states.get("velocity1"))
                    .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
                    .within(1, TimeUnit.HOURS)
                    .build();

    public static final Pattern<String, GeneralEvent> SEQ_2_WITH_STRICT_CONTIGUITY =
            new QueryBuilder<String, GeneralEvent>()
                    .select("stage-1", Selected.withStrictContiguity().withTopic("velocities"))
                    .where((event, states) -> event.value().velocity > 175.0)
                    .<Double>fold("velocity1", (k, v, curr) -> v.velocity)
                    .<Long>fold("timestamp", (k, v, curr) -> v.timestamp)
                    .then()
                    .select("stage-2", Selected.withStrictContiguity().withTopic("velocities"))
                    .where((event, states) -> event.value().velocity > 175.0)
                    .and((event, states) -> event.value().velocity >= (Double) states.get("velocity1"))
                    .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
                    .then()
                    .select("stage-3", Selected.withStrictContiguity().withTopic("quantities"))
                    .where((event, states) -> event.value().quantity < 200)
                    .and((event, states) -> event.value().quantity < (Double) states.get("velocity1"))
                    .and((event, states) -> event.value().timestamp - (long)states.get("timestamp") < 25)
                    .within(1, TimeUnit.HOURS)
                    .build();


    public static double checkDistance(GeneralEvent event, GeneralEvent event2) {
        double dx = 71.5 * (event.longitude - event2.longitude);
        double dy = 111.3 * (event.latitude - event2.latitude);
        return Math.sqrt(dx * dx + dy * dy);
    }
}
