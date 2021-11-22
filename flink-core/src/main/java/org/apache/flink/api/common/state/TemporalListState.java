/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Experimental;

/**
 * {@link State} interface for partitioned list state in Operations. The state is accessed and
 * modified by user functions, and checkpointed consistently by the system as part of the
 * distributed snapshots.
 *
 * <p>The state can be a keyed list state or an operator list state.
 *
 * <p>When it is a keyed list state, it is accessed by functions applied on a {@code KeyedStream}.
 * The key is automatically supplied by the system, so the function always sees the value mapped to
 * the key of the current element. That way, the system can handle stream and state partitioning
 * consistently together.
 *
 * <p>When it is an operator list state, the list is a collection of state items that are
 * independent of each other and eligible for redistribution across operator instances in case of
 * changed operator parallelism.
 *
 * @param <T> Type of values that this list state keeps.
 */
@Experimental
public interface TemporalListState<T>
        extends MergingState<TimestampedValue<T>, Iterable<TimestampedValue<T>>> {

    /**
     * Read a timestamp-limited subrange of the list. The result is ordered by timestamp.
     *
     * <p>All values with timestamps >= minTimestamp and < limitTimestamp will be in the resuling
     * iterable. This means that only timestamps strictly less than
     * Instant.ofEpochMilli(Long.MAX_VALUE) can be used as timestamps.
     */
    Iterable<TimestampedValue<T>> readRange(long minTimestamp, long limitTimestamp);

    /**
     * Clear a timestamp-limited subrange of the list.
     *
     * <p>All values with timestamps >= minTimestamp and < limitTimestamp will be removed from the
     * list.
     */
    void clearRange(long minTimestamp, long limitTimestamp);
}
