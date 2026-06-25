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

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
 *   <li>{@code LongestWaitingQueryDurationSeconds} equals the longest
 *       {@code resourceWaitingTime} among queries in {@code WAITING_FOR_RESOURCES}</li>
 * </ul>
 *
 * <p>Runs ≥100 iterations via JUnit 5 {@link ParameterizedTest} with
 * {@link MethodSource}.
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
    {
        Random random = new Random(seed);

        // Controllable list of queries that simulates QueryManager.getQueries()
        List<BasicQueryInfo> currentQueries = new ArrayList<>();
        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() -> ImmutableList.copyOf(currentQueries));

        // Track queries in WAITING_FOR_RESOURCES state manually for assertion
        List<QueryEntry> waitingQueries = new ArrayList<>();
        int nextQueryId = 0;

        // Execute random sequence of transitions
        int steps = 10 + random.nextInt(20);
        for (int step = 0; step < steps; step++) {
            int action = random.nextInt(3);
            switch (action) {
                case 0 -> {
                    // Enqueue: add a query in WAITING_FOR_RESOURCES state
                    String id = "query_" + nextQueryId++;
                    long resourceWaitMillis = random.nextInt(10000) + 100; // 100ms to 10s
                    currentQueries.add(createQueryInfo(id, WAITING_FOR_RESOURCES, resourceWaitMillis));
                    waitingQueries.add(new QueryEntry(id, resourceWaitMillis));
                }
                case 1 -> {
                    // Dequeue: move a waiting query to RUNNING (remove from waiting set)
                    if (!waitingQueries.isEmpty()) {
                        QueryEntry removed = waitingQueries.remove(random.nextInt(waitingQueries.size()));
                        currentQueries.removeIf(query -> query.getQueryId().toString().equals(removed.queryId()));
                        currentQueries.add(createQueryInfo(removed.queryId(), RUNNING, 0));
                    }
                }
                case 2 -> {
                    // Fail: move a waiting query to FINISHED (remove from waiting set)
                    if (!waitingQueries.isEmpty()) {
                        QueryEntry removed = waitingQueries.remove(random.nextInt(waitingQueries.size()));
                        currentQueries.removeIf(query -> query.getQueryId().toString().equals(removed.queryId()));
                        currentQueries.add(createQueryInfo(removed.queryId(), FINISHED, 0));
                    }
                }
            }

            assertThat(monitor.getWaitingQueryCount())
                    .as("WaitingQueryCount at step %d (seed=%d)", step, seed)
                    .isEqualTo(waitingQueries.size());

            long expectedMaxMillis = waitingQueries.stream()
                    .mapToLong(QueryEntry::resourceWaitMillis)
                    .max()
                    .orElse(0L);
            assertThat(monitor.getLongestWaitingQueryDurationSeconds())
                    .as("LongestWaitingQueryDurationSeconds at step %d (seed=%d)", step, seed)
                    .isEqualTo(expectedMaxMillis / 1e3);
        }
    }

    static BasicQueryInfo createQueryInfo(String queryId, QueryState state, long resourceWaitMillis)
    {
        Duration zeroDuration = new Duration(0, TimeUnit.MILLISECONDS);
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
                new BasicQueryStats(
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
                        OptionalDouble.empty()),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }

    private record QueryEntry(String queryId, long resourceWaitMillis) {}
}
