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
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that {@link AdmissionStateMonitor} gauge readings depend only on
 * {@link QueryManager} state and not on any admission-policy state.
 *
 * <p>Since PR 1 (AdmissionPolicy SPI) has not been applied, this is trivially true:
 * the monitor class has no compile-time or runtime dependency on any admission policy
 * type. This test verifies that:
 * <ul>
 *   <li>The monitor class does not import or reference any admission policy classes</li>
 *   <li>Gauge readings are purely derived from QueryManager.getQueries() results</li>
 *   <li>Given identical QueryManager state, gauges always produce identical readings</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 7.4, 7.6</b>
 */
class TestAdmissionStateMonitorPolicyAgnostic
{
    @Test
    void testMonitorHasNoAdmissionPolicyDependency()
    {
        // Verify that AdmissionStateMonitor does not import or reference any
        // admission policy types. Check declared fields and method signatures.
        Class<?> monitorClass = AdmissionStateMonitor.class;

        // Check all declared fields — none should reference admission policy types
        for (Field field : monitorClass.getDeclaredFields()) {
            String typeName = field.getType().getName();
            assertThat(typeName)
                    .as("Field %s should not reference admission policy types", field.getName())
                    .doesNotContain("admission")
                    .doesNotContain("AdmissionPolicy");
        }

        // Check constructor parameters — none should reference admission policy types
        Arrays.stream(monitorClass.getDeclaredConstructors()).forEach(constructor -> {
            for (Class<?> paramType : constructor.getParameterTypes()) {
                assertThat(paramType.getName())
                        .as("Constructor parameter should not reference admission policy types")
                        .doesNotContain("admission")
                        .doesNotContain("AdmissionPolicy");
            }
        });

        // Check method return types and parameter types
        Arrays.stream(monitorClass.getDeclaredMethods()).forEach(method -> {
            assertThat(method.getReturnType().getName())
                    .as("Method %s return type should not reference admission policy", method.getName())
                    .doesNotContain("admission")
                    .doesNotContain("AdmissionPolicy");
            for (Class<?> paramType : method.getParameterTypes()) {
                assertThat(paramType.getName())
                        .as("Method %s parameter should not reference admission policy", method.getName())
                        .doesNotContain("admission")
                        .doesNotContain("AdmissionPolicy");
            }
        });
    }

    @Test
    void testGaugesArePurelyDerivedFromQueryManagerState()
            throws Exception
    {
        // Given a known QueryManager state, verify gauges produce the same readings
        // regardless of how many times they are called or what order they are read in.
        // This proves the gauges are stateless and policy-agnostic.
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("query_1", WAITING_FOR_RESOURCES, 5000),
                createQueryInfo("query_2", RUNNING, 0),
                createQueryInfo("query_3", WAITING_FOR_RESOURCES, 3000),
                createQueryInfo("query_4", QUEUED, 0));

        AdmissionStateMonitor monitor = createMonitorWithQueries(() -> queries);

        // Read gauges multiple times — they should always return the same values
        for (int i = 0; i < 10; i++) {
            assertThat(monitor.getWaitingQueryCount())
                    .as("WaitingQueryCount should be consistent on read %d", i)
                    .isEqualTo(2);

            assertThat(monitor.getLongestWaitingQueryDurationSeconds())
                    .as("LongestWaitingQueryDurationSeconds should be consistent on read %d", i)
                    .isEqualTo(5.0);
        }
    }

    @Test
    void testGaugesWithEmptyQueryList()
            throws Exception
    {
        // Verify gauges report zero when no queries exist
        AdmissionStateMonitor monitor = createMonitorWithQueries(ImmutableList::of);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugesWithNoWaitingQueries()
            throws Exception
    {
        // Verify gauges report zero when queries exist but none are WAITING_FOR_RESOURCES
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("q1", RUNNING, 1000),
                createQueryInfo("q2", QUEUED, 500));

        AdmissionStateMonitor monitor = createMonitorWithQueries(() -> queries);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugesReflectOnlyCurrentState()
            throws Exception
    {
        // Verify that gauges reflect only the current state returned by getQueries(),
        // with no hidden internal state or caching. Change the supplier and verify
        // gauges immediately reflect the new state.
        List<BasicQueryInfo> initialQueries = new java.util.ArrayList<>(ImmutableList.of(
                createQueryInfo("q1", WAITING_FOR_RESOURCES, 2000)));

        Supplier<List<BasicQueryInfo>> supplier = () -> ImmutableList.copyOf(initialQueries);
        AdmissionStateMonitor monitor = createMonitorWithQueries(supplier);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(1);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(2.0);

        // "Dequeue" the query — change the backing list
        initialQueries.clear();
        initialQueries.add(createQueryInfo("q1", RUNNING, 0));

        // Gauges should immediately reflect the new state — no stale caching
        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    private static AdmissionStateMonitor createMonitorWithQueries(Supplier<List<BasicQueryInfo>> querySupplier)
            throws Exception
    {
        Ticker ticker = Ticker.systemTicker();

        sun.misc.Unsafe unsafe = getUnsafe();
        AdmissionStateMonitor monitor = (AdmissionStateMonitor) unsafe.allocateInstance(AdmissionStateMonitor.class);

        // Set ticker field
        Field tickerField = AdmissionStateMonitor.class.getDeclaredField("ticker");
        tickerField.setAccessible(true);
        tickerField.set(monitor, ticker);

        // Create a TestableQueryManager via Unsafe and set the supplier
        QueryManager queryManager = (QueryManager) unsafe.allocateInstance(TestAdmissionStateMonitor.TestableQueryManager.class);
        Field supplierField = TestAdmissionStateMonitor.TestableQueryManager.class.getDeclaredField("querySupplier");
        supplierField.setAccessible(true);
        supplierField.set(queryManager, querySupplier);

        // Set queryManager field on monitor
        Field queryManagerField = AdmissionStateMonitor.class.getDeclaredField("queryManager");
        queryManagerField.setAccessible(true);
        queryManagerField.set(monitor, queryManager);

        return monitor;
    }

    private static sun.misc.Unsafe getUnsafe()
            throws Exception
    {
        Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (sun.misc.Unsafe) unsafeField.get(null);
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, long resourceWaitMillis)
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
}
