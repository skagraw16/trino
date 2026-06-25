/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.spi.QueryId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-style test for {@link AdmissionStateMonitor}.
 *
 * <p>Generates sequences of (enqueue, dequeue, fail) state transitions over a
 * controllable query list. After each step, asserts:
 * <ul>
 *   <li>{@code WaitingQueryCount} equals the in-state set size</li>
 *   <li>{@code LongestWaitingQueryDurationSeconds} equals the oldest-enter-state
 *       delta (derived from resourceWaitingTime on the query stats)</li>
 * </ul>
 *
 * <p>Uses a deterministic injected {@link Ticker} and ≥100 iterations via
 * JUnit 5 {@link ParameterizedTest} with {@link MethodSource}.
 *
 * <p><b>Validates: Requirements 7.1, 7.2, 7.3, 7.4</b>
 */
class TestAdmissionStateMonitor
{
    /**
     * Generates 120 test scenarios with different random seeds, each producing
     * a different sequence of state transitions.
     */
    static Stream<Long> transitionSequenceSeeds()
    {
        return Stream.iterate(1L, seed -> seed + 1).limit(120);
    }

    @ParameterizedTest
    @MethodSource("transitionSequenceSeeds")
    void testGaugeCorrectnessAcrossStateTransitions(long seed)
            throws Exception
    {
        Random random = new Random(seed);
        AtomicLong currentTimeNanos = new AtomicLong(TimeUnit.SECONDS.toNanos(1_000_000));

        Ticker testTicker = new Ticker()
        {
            @Override
            public long read()
            {
                return currentTimeNanos.get();
            }
        };

        // Controllable list of queries that simulates QueryManager.getQueries()
        List<BasicQueryInfo> currentQueries = new ArrayList<>();
        Supplier<List<BasicQueryInfo>> querySupplier = () -> ImmutableList.copyOf(currentQueries);

        // Create AdmissionStateMonitor using reflection to inject a controllable supplier
        AdmissionStateMonitor monitor = createMonitorWithSupplier(querySupplier, testTicker);

        // Track queries in WAITING_FOR_RESOURCES state manually for assertion
        List<QueryEntry> waitingQueries = new ArrayList<>();
        int nextQueryId = 0;

        // Execute random sequence of transitions
        int steps = 10 + random.nextInt(20);
        for (int step = 0; step < steps; step++) {
            // Advance time by a random amount (0-5 seconds)
            long advanceNanos = TimeUnit.MILLISECONDS.toNanos(random.nextInt(5000));
            currentTimeNanos.addAndGet(advanceNanos);

            int action = random.nextInt(3);
            switch (action) {
                case 0 -> {
                    // Enqueue: add a query in WAITING_FOR_RESOURCES state
                    String id = "query_" + nextQueryId++;
                    long resourceWaitMillis = random.nextInt(10000) + 100; // 100ms to 10s
                    BasicQueryInfo queryInfo = createQueryInfo(id, WAITING_FOR_RESOURCES, resourceWaitMillis);
                    currentQueries.add(queryInfo);
                    waitingQueries.add(new QueryEntry(id, resourceWaitMillis));
                }
                case 1 -> {
                    // Dequeue: move a waiting query to RUNNING (remove from waiting set)
                    if (!waitingQueries.isEmpty()) {
                        int index = random.nextInt(waitingQueries.size());
                        QueryEntry removed = waitingQueries.remove(index);
                        // Replace the query in currentQueries with a RUNNING version
                        currentQueries.removeIf(q -> q.getQueryId().toString().equals(removed.queryId));
                        currentQueries.add(createQueryInfo(removed.queryId, RUNNING, 0));
                    }
                }
                case 2 -> {
                    // Fail: move a waiting query to FINISHED (remove from waiting set)
                    if (!waitingQueries.isEmpty()) {
                        int index = random.nextInt(waitingQueries.size());
                        QueryEntry removed = waitingQueries.remove(index);
                        // Replace the query in currentQueries with a FINISHED version
                        currentQueries.removeIf(q -> q.getQueryId().toString().equals(removed.queryId));
                        currentQueries.add(createQueryInfo(removed.queryId, FINISHED, 0));
                    }
                }
            }

            // Assert WaitingQueryCount
            assertThat(monitor.getWaitingQueryCount())
                    .as("WaitingQueryCount at step %d (seed=%d)", step, seed)
                    .isEqualTo(waitingQueries.size());

            // Assert LongestWaitingQueryDurationSeconds
            long expectedMaxMillis = waitingQueries.stream()
                    .mapToLong(entry -> entry.resourceWaitMillis)
                    .max()
                    .orElse(0L);
            double expectedSeconds = expectedMaxMillis / 1e3;

            assertThat(monitor.getLongestWaitingQueryDurationSeconds())
                    .as("LongestWaitingQueryDurationSeconds at step %d (seed=%d)", step, seed)
                    .isEqualTo(expectedSeconds);
        }
    }

    /**
     * Creates an AdmissionStateMonitor that uses a controllable query supplier
     * instead of a real QueryManager. Uses reflection to set the queryManager field
     * to a lightweight wrapper that delegates getQueries() to the supplier.
     */
    private static AdmissionStateMonitor createMonitorWithSupplier(
            Supplier<List<BasicQueryInfo>> querySupplier,
            Ticker ticker)
            throws Exception
    {
        // Create a minimal QueryManager subclass via an anonymous approach.
        // Since QueryManager requires complex dependencies, we use reflection
        // to create the AdmissionStateMonitor and set its fields directly.
        AdmissionStateMonitor monitor = allocateInstance(AdmissionStateMonitor.class);

        // Set the ticker field
        Field tickerField = AdmissionStateMonitor.class.getDeclaredField("ticker");
        tickerField.setAccessible(true);
        tickerField.set(monitor, ticker);

        // Create a QueryManager proxy using a subclass approach via Unsafe
        // Since we only need getQueries(), create a delegating wrapper
        QueryManager queryManagerProxy = createQueryManagerProxy(querySupplier);

        Field queryManagerField = AdmissionStateMonitor.class.getDeclaredField("queryManager");
        queryManagerField.setAccessible(true);
        queryManagerField.set(monitor, queryManagerProxy);

        return monitor;
    }

    /**
     * Creates a QueryManager instance that returns controlled query data.
     * Uses sun.misc.Unsafe to allocate without calling the constructor.
     */
    @SuppressWarnings("unchecked")
    private static QueryManager createQueryManagerProxy(Supplier<List<BasicQueryInfo>> querySupplier)
            throws Exception
    {
        // Use sun.misc.Unsafe to allocate a QueryManager instance without
        // invoking its constructor (which requires complex dependencies).
        // We then wrap it in a subclass-like approach via a dynamic proxy.
        // Since QueryManager is a concrete class, we use Unsafe.allocateInstance.
        sun.misc.Unsafe unsafe = getUnsafe();
        QueryManager instance = (QueryManager) unsafe.allocateInstance(TestableQueryManager.class);

        // Set the query supplier on our testable subclass
        Field supplierField = TestableQueryManager.class.getDeclaredField("querySupplier");
        supplierField.setAccessible(true);
        supplierField.set(instance, querySupplier);

        return instance;
    }

    private static sun.misc.Unsafe getUnsafe()
            throws Exception
    {
        Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (sun.misc.Unsafe) unsafeField.get(null);
    }

    @SuppressWarnings("unchecked")
    private static <T> T allocateInstance(Class<T> clazz)
            throws Exception
    {
        return (T) getUnsafe().allocateInstance(clazz);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, long resourceWaitMillis)
    {
        return new BasicQueryInfo(
                new QueryId(queryId),
                testSessionBuilder().build().toSessionRepresentation(),
                Optional.empty(),
                state,
                state == RUNNING,
                URI.create("http://localhost/query/" + queryId),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                createBasicQueryStats(resourceWaitMillis),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }

    private static BasicQueryStats createBasicQueryStats(long resourceWaitMillis)
    {
        Duration zeroDuration = new Duration(0, TimeUnit.MILLISECONDS);
        return new BasicQueryStats(
                Instant.now(),
                Instant.now(),
                zeroDuration,
                new Duration(resourceWaitMillis, TimeUnit.MILLISECONDS),
                zeroDuration,
                zeroDuration,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0.0,
                0.0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                false,
                ImmutableSet.of(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    /**
     * A subclass of QueryManager that overrides getQueries() to return
     * a controlled list from a supplier. Created via Unsafe.allocateInstance
     * to bypass the complex constructor dependencies.
     */
    static class TestableQueryManager
            extends QueryManager
    {
        @SuppressWarnings("unused")
        private Supplier<List<BasicQueryInfo>> querySupplier;

        // Constructor is never called (instance is allocated via Unsafe)
        // but must exist for the class to be valid
        private TestableQueryManager()
        {
            super(null, null, null);
        }

        @Override
        public List<BasicQueryInfo> getQueries()
        {
            return querySupplier.get();
        }
    }

    private record QueryEntry(String queryId, long resourceWaitMillis) {}
}
