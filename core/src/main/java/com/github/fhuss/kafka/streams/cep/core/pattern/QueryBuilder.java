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
package com.github.fhuss.kafka.streams.cep.core.pattern;

/**
 * Default class to build a new complex event query.
 *
 * @param <K>   the record key type.
 * @param <V>   the record value type.
 */
public class QueryBuilder<K, V> {

    private static final Selected DEFAULT_SELECT_STRATEGY = Selected.withStrictContiguity();

    /**
     * Creates a new stage with no name.
     *
     * @return a new {@link StageBuilder}.
     */
    public StageBuilder<K, V> select() {
        return select(DEFAULT_SELECT_STRATEGY);
    }

    /**
     * Creates a new stage with the specified name.
     *
     * @param name the stage name.
     * @return a new {@link StageBuilder}.
     */
    public StageBuilder<K, V> select(final String name) {
        return select(name, DEFAULT_SELECT_STRATEGY);
    }

    /**
     * Creates a new stage with no name.
     * @param selected  the instance of {@link Selected} used to define optional parameters
     * @return a new {@link StageBuilder}.
     */
    public StageBuilder<K, V> select(final Selected selected) {
        return new StageBuilder<>(new Pattern<>(selected));
    }

    public StageBuilder<K, V> select(final String name, final Selected selected) {
        return new StageBuilder<>(new Pattern<>(name, selected));
    }
}
