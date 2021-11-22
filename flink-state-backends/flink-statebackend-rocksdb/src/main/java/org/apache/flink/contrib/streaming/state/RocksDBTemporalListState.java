/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.TimestampedValue;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalTemporalListState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.flink.shaded.guava30.com.google.common.collect.AbstractIterator;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * {@link ListState} implementation that stores state in RocksDB.
 *
 * <p>{@link EmbeddedRocksDBStateBackend} must ensure that we set the {@link
 * org.rocksdb.StringAppendOperator} on the column family that we use for our state since we use the
 * {@code merge()} call.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
class RocksDBTemporalListState<K, N, V>
        extends AbstractRocksDBState<K, N, List<TimestampedValue<V>>>
        implements InternalTemporalListState<K, N, V> {

    /** Serializer for the values. */
    private final TypeSerializer<TimestampedValue<V>> elementSerializer;

    private final ListDelimitedSerializer listSerializer;

    /** Separator of StringAppendTestOperator in RocksDB. */
    private static final byte DELIMITER = ',';

    /**
     * Compares two byte arrays lexicographically.
     *
     * <p>Can be replaced by {@code Arrays#compare(byte[],byte[])} once we drop Java 1.8 support.
     *
     * @param a first array to compare
     * @param b second array to compare
     * @return the value 0 if the first and second array are equal and contain the same elements in
     *     the same order; a value less than 0 if the first array is lexicographically less than the
     *     second array; and a value greater than 0 if the first array is lexicographically greater
     *     than the second array
     */
    private static int compareBytes(byte[] a, byte[] b) {
        final ByteBuffer ab = ByteBuffer.wrap(a);
        final ByteBuffer bb = ByteBuffer.wrap(b);
        return ab.compareTo(bb);
    }

    private static class RangeIterator<V> extends AbstractIterator<TimestampedValue<V>> {

        private final RocksIterator iterator;
        private final TypeSerializer<TimestampedValue<V>> elementSerializer;
        private final ListDelimitedSerializer listSerializer;
        private final DataInputDeserializer dataInputView;
        private final byte[] endKey;

        private List<TimestampedValue<V>> nextValues = null;

        public RangeIterator(
                RocksIterator iterator,
                TypeSerializer<TimestampedValue<V>> elementSerializer,
                ListDelimitedSerializer listSerializer,
                DataInputDeserializer dataInputView,
                byte[] endKey) {
            this.iterator = iterator;
            this.elementSerializer = elementSerializer;
            this.listSerializer = listSerializer;
            this.dataInputView = dataInputView;
            this.endKey = endKey;
        }

        @Override
        protected TimestampedValue<V> computeNext() {
            if (nextValues != null && !nextValues.isEmpty()) {
                return nextValues.remove(0);
            }
            if (!iterator.isValid() || compareBytes(iterator.key(), endKey) > 0) {
                iterator.close();
                return endOfData();
            }
            dataInputView.setBuffer(iterator.value());
            nextValues = listSerializer.deserializeList(iterator.value(), elementSerializer);
            iterator.next();
            return computeNext();
        }
    }

    /**
     * Creates a new {@code RocksDBListState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBTemporalListState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<List<TimestampedValue<V>>> valueSerializer,
            List<TimestampedValue<V>> defaultValue,
            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
        final ListSerializer<TimestampedValue<V>> listSerializer =
                (ListSerializer<TimestampedValue<V>>) valueSerializer;
        this.elementSerializer = listSerializer.getElementSerializer();
        this.listSerializer = new ListDelimitedSerializer();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<List<TimestampedValue<V>>> getValueSerializer() {
        return valueSerializer;
    }

    private byte[] createKey(long timestamp) {
        try {
            return serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
                    timestamp, LongSerializer.INSTANCE);
        } catch (IOException shouldNeverHappen) {
            throw new FlinkRuntimeException(shouldNeverHappen);
        }
    }

    @Override
    public Iterable<TimestampedValue<V>> readRange(long minTimestamp, long limitTimestamp) {
        final RocksIterator rocksIterator =
                backend.db.newIterator(columnFamily, backend.getReadOptions());
        final byte[] startKey = createKey(minTimestamp);
        final byte[] endKey = createKey(limitTimestamp);
        rocksIterator.seek(startKey);
        if (!rocksIterator.isValid() || compareBytes(rocksIterator.key(), endKey) > 0) {
            rocksIterator.close();
            return null;
        }
        return () ->
                new RangeIterator<>(
                        rocksIterator,
                        elementSerializer,
                        listSerializer,
                        dataInputView,
                        createKey(limitTimestamp));
    }

    @Override
    public void clearRange(long minTimestamp, long limitTimestamp) {
        try {
            backend.db.deleteRange(
                    columnFamily, createKey(minTimestamp), createKey(limitTimestamp));
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException("Error while clearing range from RocksDB", e);
        }
    }

    @Override
    public Iterable<TimestampedValue<V>> get() {
        return readRange(0L, Long.MAX_VALUE);
    }

    @Override
    public List<TimestampedValue<V>> getInternal() throws Exception {
        final List<TimestampedValue<V>> materialized = new ArrayList<>();
        get().forEach(materialized::add);
        return materialized;
    }

    @Override
    public void add(TimestampedValue<V> value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        try {
            backend.db.merge(
                    columnFamily,
                    writeOptions,
                    createKey(value.getTimestamp()),
                    serializeValue(value, elementSerializer));
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        // TODO
    }

    @Override
    public void updateInternal(List<TimestampedValue<V>> values) {
        // TODO
    }

    @Override
    public void migrateSerializedValue(
            DataInputDeserializer serializedOldValueInput,
            DataOutputSerializer serializedMigratedValueOutput,
            TypeSerializer<List<TimestampedValue<V>>> priorSerializer,
            TypeSerializer<List<TimestampedValue<V>>> newSerializer)
            throws StateMigrationException {
        // TODO
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBTemporalListState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        (TypeSerializer<List<TimestampedValue<E>>>)
                                registerResult.f1.getStateSerializer(),
                        (List<TimestampedValue<E>>) stateDesc.getDefaultValue(),
                        backend);
    }
}
