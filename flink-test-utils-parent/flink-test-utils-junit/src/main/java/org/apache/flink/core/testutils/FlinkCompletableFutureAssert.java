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

package org.apache.flink.core.testutils;

import org.assertj.core.api.AbstractCompletableFutureAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableAssertAlternative;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Enhanced version of {@link org.assertj.core.api.CompletableFutureAssert}, that allows asserting
 * futures without relying on timeouts.
 *
 * @param <T> type of the value contained in the {@link CompletableFuture}.
 */
public class FlinkCompletableFutureAssert<T>
        extends AbstractCompletableFutureAssert<FlinkCompletableFutureAssert<T>, T> {

    FlinkCompletableFutureAssert(CompletableFuture<T> actual) {
        super(actual, FlinkCompletableFutureAssert.class);
    }

    FlinkCompletableFutureAssert(CompletionStage<T> actual) {
        super(actual.toCompletableFuture(), FlinkCompletableFutureAssert.class);
    }

    /**
     * An equivalent of {@link #succeedsWithin(Duration)}, that doesn't rely on timeouts.
     *
     * @return a new assertion object on the future's result
     */
    public ObjectAssert<T> eventuallySucceeds() {
        final T object = assertEventuallySucceeds(info, actual);
        return new ObjectAssert<>(object);
    }

    /**
     * An equivalent of {@link #failsWithin(Duration)}, that doesn't rely on timeouts.
     *
     * @param exceptionClass type of the exception we expect the future to complete with
     * @return a new assertion instance on the future's exception.
     * @param <E> type of the exception we expect the future to complete with
     */
    public <E extends Throwable> ThrowableAssertAlternative<E> eventuallyFailsWith(
            Class<E> exceptionClass) {
        final Exception exception = assertEventuallyFails(info, actual);
        final ThrowableAssertAlternative<Exception> throwableAssert =
                new ThrowableAssertAlternative<>(exception).isInstanceOf(exceptionClass);
        @SuppressWarnings("unchecked")
        final ThrowableAssertAlternative<E> cast = (ThrowableAssertAlternative<E>) throwableAssert;
        return cast;
    }

    private T assertEventuallySucceeds(AssertionInfo info, Future<T> actual) {
        Objects.instance().assertNotNull(info, actual);
        try {
            return actual.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            throw Failures.instance().failure(info, new BasicErrorMessageFactory("x"));
        }
    }

    private Exception assertEventuallyFails(AssertionInfo info, Future<?> actual) {
        Objects.instance().assertNotNull(info, actual);
        try {
            actual.get();
            throw Failures.instance().failure(info, new BasicErrorMessageFactory(""));
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            return e;
        }
    }
}
