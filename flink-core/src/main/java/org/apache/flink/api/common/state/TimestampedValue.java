package org.apache.flink.api.common.state;

import org.apache.flink.shaded.guava30.com.google.common.base.MoreObjects;

import java.util.Comparator;

public class TimestampedValue<T> implements Comparable<TimestampedValue<T>> {

    private final T value;
    private final long timestamp;

    public TimestampedValue(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public T getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(TimestampedValue<T> o) {
        return Comparator.<TimestampedValue<T>>comparingLong(TimestampedValue::getTimestamp)
                .compare(this, o);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", value)
                .add("timestamp", timestamp)
                .toString();
    }
}
