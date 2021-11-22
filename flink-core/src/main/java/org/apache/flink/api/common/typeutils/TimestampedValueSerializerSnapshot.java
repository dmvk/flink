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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.state.TimestampedValue;
import org.apache.flink.api.common.typeutils.base.TimestampedValueSerializer;

/** Snapshot class for the {@link TimestampedValueSerializer}. */
public class TimestampedValueSerializerSnapshot<T>
        extends CompositeTypeSerializerSnapshot<
                TimestampedValue<T>, TimestampedValueSerializer<T>> {

    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    public TimestampedValueSerializerSnapshot() {
        super(TimestampedValueSerializer.class);
    }

    /** Constructor to create the snapshot for writing. */
    public TimestampedValueSerializerSnapshot(TimestampedValueSerializer<T> serializer) {
        super(serializer);
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected TimestampedValueSerializer<T> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        final TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
        return new TimestampedValueSerializer<>(elementSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
            TimestampedValueSerializer<T> outerSerializer) {
        return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
    }
}
