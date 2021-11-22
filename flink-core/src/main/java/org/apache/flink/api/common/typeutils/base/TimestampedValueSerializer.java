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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.TimestampedValue;
import org.apache.flink.api.common.typeutils.TimestampedValueSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for {@link List Lists}. The serializer relies on an element serializer for the
 * serialization of the list's elements.
 *
 * <p>The serialization format for the list is as follows: four bytes for the length of the lost,
 * followed by the serialized representation of each element.
 *
 * @param <T> The type of element in the list.
 */
@Internal
public final class TimestampedValueSerializer<T> extends TypeSerializer<TimestampedValue<T>> {

    private static final long serialVersionUID = 1;

    /** The serializer for the elements of the list. */
    private final TypeSerializer<T> elementSerializer;

    /**
     * Creates a list serializer that uses the given serializer to serialize the list's elements.
     *
     * @param elementSerializer The serializer for the elements of the list
     */
    public TimestampedValueSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = checkNotNull(elementSerializer);
    }

    // ------------------------------------------------------------------------
    //  ListSerializer specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the serializer for the elements of the list.
     *
     * @return The serializer for the elements of the list
     */
    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return elementSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<TimestampedValue<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer
                ? this
                : new TimestampedValueSerializer<>(duplicateElement);
    }

    @Override
    public TimestampedValue<T> createInstance() {
        return new TimestampedValue<>(elementSerializer.createInstance(), Long.MAX_VALUE);
    }

    @Override
    public TimestampedValue<T> copy(TimestampedValue<T> from) {
        if (isImmutableType()) {
            return from;
        }
        return new TimestampedValue<>(elementSerializer.copy(from.getValue()), from.getTimestamp());
    }

    @Override
    public TimestampedValue<T> copy(TimestampedValue<T> from, TimestampedValue<T> reuse) {
        if (isImmutableType()) {
            return from;
        }
        return new TimestampedValue<>(
                elementSerializer.copy(from.getValue(), reuse.getValue()), from.getTimestamp());
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(TimestampedValue<T> timestampedValue, DataOutputView target)
            throws IOException {
        elementSerializer.serialize(timestampedValue.getValue(), target);
        target.writeLong(timestampedValue.getTimestamp());
    }

    @Override
    public TimestampedValue<T> deserialize(DataInputView source) throws IOException {
        final T value = elementSerializer.deserialize(source);
        final long timestamp = source.readLong();
        return new TimestampedValue<>(value, timestamp);
    }

    @Override
    public TimestampedValue<T> deserialize(TimestampedValue<T> reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        elementSerializer.copy(source, target);
        final long timestamp = source.readLong();
        target.writeLong(timestamp);
    }

    // --------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(
                                ((TimestampedValueSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<TimestampedValue<T>> snapshotConfiguration() {
        return new TimestampedValueSerializerSnapshot<>(this);
    }
}
