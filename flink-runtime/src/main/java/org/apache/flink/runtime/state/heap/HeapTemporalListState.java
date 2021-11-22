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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.TemporalListState;
import org.apache.flink.api.common.state.TimestampedValue;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalTemporalListState;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Heap-backed partitioned {@link TemporalListState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class HeapTemporalListState<K, N, V>
        extends AbstractHeapMergingState<
                K, N, TimestampedValue<V>, List<TimestampedValue<V>>, Iterable<TimestampedValue<V>>>
        implements InternalTemporalListState<K, N, V> {

    @VisibleForTesting
    static <T> void insertSorted(ArrayList<T> array, T element, Comparator<T> comparator) {
        array.add(element);
        int j = array.size() - 1;
        while (j > 0 && comparator.compare(array.get(j - 1), element) > 0) {
            array.set(j, array.get(j - 1));
            j--;
        }
        array.set(j, element);
    }

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    private HeapTemporalListState(
            StateTable<K, N, List<TimestampedValue<V>>> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<List<TimestampedValue<V>>> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            List<TimestampedValue<V>> defaultValue) {
        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<List<TimestampedValue<V>>> getValueSerializer() {
        return valueSerializer;
    }

    // ------------------------------------------------------------------------
    //  state access
    // ------------------------------------------------------------------------

    @Override
    public Iterable<TimestampedValue<V>> get() {
        return getInternal();
    }

    @Override
    public void add(TimestampedValue<V> value) {
        Preconditions.checkNotNull(value, "You cannot add null to a TemporalListState.");
        final N namespace = currentNamespace;
        final StateTable<K, N, List<TimestampedValue<V>>> map = stateTable;
        List<TimestampedValue<V>> list = map.get(namespace);
        if (list == null) {
            list = new ArrayList<>();
            map.put(namespace, list);
        }
        insertSorted(
                (ArrayList<TimestampedValue<V>>) list,
                value,
                Comparator.comparingLong(TimestampedValue::getTimestamp));
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<List<TimestampedValue<V>>> safeValueSerializer)
            throws Exception {
        // We can reuse the list state here...
        final HeapListState<K, N, TimestampedValue<V>> listState =
                new HeapListState<>(
                        stateTable,
                        safeKeySerializer,
                        safeValueSerializer,
                        safeNamespaceSerializer,
                        getDefaultValue());
        return listState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public Iterable<TimestampedValue<V>> readRange(long minTimestamp, long limitTimestamp) {
        final List<TimestampedValue<V>> internal = getInternal();
        final int minIdx =
                Collections.binarySearch(internal, new TimestampedValue<>(null, minTimestamp));
        final int maxIdx =
                Collections.binarySearch(internal, new TimestampedValue<>(null, limitTimestamp));
        return internal.subList(minIdx, maxIdx + 1);
    }

    @Override
    public void clearRange(long minTimestamp, long limitTimestamp) {
        final List<TimestampedValue<V>> internal = getInternal();
        final int minIdx =
                Collections.binarySearch(internal, new TimestampedValue<>(null, minTimestamp));
        final int maxIdx =
                Collections.binarySearch(internal, new TimestampedValue<>(null, limitTimestamp));
        internal.subList(minIdx, maxIdx).clear();
    }

    // ------------------------------------------------------------------------
    //  state merging
    // ------------------------------------------------------------------------

    @Override
    protected List<TimestampedValue<V>> mergeState(
            List<TimestampedValue<V>> a, List<TimestampedValue<V>> b) {
        final List<TimestampedValue<V>> merged = new ArrayList<>();
        Iterables.mergeSorted(
                        Arrays.asList(a, b),
                        Comparator.comparingLong(TimestampedValue::getTimestamp))
                .forEach(merged::add);
        return merged;
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new HeapTemporalListState<>(
                        (StateTable<K, N, List<TimestampedValue<E>>>) stateTable,
                        keySerializer,
                        (TypeSerializer<List<TimestampedValue<E>>>) stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        (List<TimestampedValue<E>>) stateDesc.getDefaultValue());
    }
}
