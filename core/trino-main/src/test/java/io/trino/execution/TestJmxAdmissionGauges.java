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
import org.weakref.jmx.Managed;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that {@link AdmissionStateMonitor} is properly
 * structured for JMX export. Validates that:
 * <ul>
 *   <li>The {@code @Managed} annotation is present on gauge methods</li>
 *   <li>The gauge methods have correct return types for JMX exposure</li>
 *   <li>The gauges produce accurate values when reading from a known state</li>
 *   <li>The class is suitable for binding via Guice with JMX export</li>
 * </ul>
 *
 * <p>This is a lightweight validation that the MBean contract is correct without
 * requiring a full {@code DistributedQueryRunner} boot. It verifies Req 7.6 by
 * ensuring the {@code @Managed} methods are available for JMX registration.
 *
 * <p><b>Validates: Requirements 7.1, 7.2, 7.3, 7.5, 7.6</b>
 */
class TestJmxAdmissionGauges
{
    @Test
    void testWaitingQueryCountHasManagedAnnotation()
            throws Exception
    {
        Method method = AdmissionStateMonitor.class.getMethod("getWaitingQueryCount");
        assertThat(method.isAnnotationPresent(Managed.class))
                .as("getWaitingQueryCount should be annotated with @Managed")
                .isTrue();
        assertThat(method.getReturnType())
                .as("getWaitingQueryCount return type should be int for JMX")
                .isEqualTo(int.class);
    }

    @Test
    void testLongestWaitingQueryDurationHasManagedAnnotation()
            throws Exception
    {
        Method method = AdmissionStateMonitor.class.getMethod("getLongestWaitingQueryDurationSeconds");
        assertThat(method.isAnnotationPresent(Managed.class))
                .as("getLongestWaitingQueryDurationSeconds should be annotated with @Managed")
                .isTrue();
        assertThat(method.getReturnType())
                .as("getLongestWaitingQueryDurationSeconds return type should be double for JMX")
                .isEqualTo(double.class);
    }

    @Test
    void testManagedMethodsArePublic()
            throws Exception
    {
        // JMX requires public methods to be exported
        Method waitingCount = AdmissionStateMonitor.class.getMethod("getWaitingQueryCount");
        Method longestDuration = AdmissionStateMonitor.class.getMethod("getLongestWaitingQueryDurationSeconds");

        assertThat(java.lang.reflect.Modifier.isPublic(waitingCount.getModifiers()))
                .as("getWaitingQueryCount must be public for JMX export")
                .isTrue();
        assertThat(java.lang.reflect.Modifier.isPublic(longestDuration.getModifiers()))
                .as("getLongestWaitingQueryDurationSeconds must be public for JMX export")
                .isTrue();
    }

    @Test
    void testManagedMethodsTakeNoParameters()
            throws Exception
    {
        // JMX gauge attributes must be parameterless getters
        Method waitingCount = AdmissionStateMonitor.class.getMethod("getWaitingQueryCount");
        Method longestDuration = AdmissionStateMonitor.class.getMethod("getLongestWaitingQueryDurationSeconds");

        assertThat(waitingCount.getParameterCount())
                .as("getWaitingQueryCount should take no parameters (JMX getter)")
                .isEqualTo(0);
        assertThat(longestDuration.getParameterCount())
                .as("getLongestWaitingQueryDurationSeconds should take no parameters (JMX getter)")
                .isEqualTo(0);
    }

    @Test
    void testGaugesDoNotCreateBackgroundThreads()
    {
        // Verify the class does not declare any ExecutorService, ScheduledExecutorService,
        // or Thread fields (Req 7.5: compute on-demand, no background threads)
        for (Field field : AdmissionStateMonitor.class.getDeclaredFields()) {
            String typeName = field.getType().getName();
            assertThat(typeName)
                    .as("Field %s should not be a thread/executor type", field.getName())
                    .doesNotContain("ExecutorService")
                    .doesNotContain("ScheduledExecutor")
                    .doesNotContain("java.lang.Thread");
        }
    }

    @Test
    void testGaugeAccuracyWithKnownState()
            throws Exception
    {
        // End-to-end validation: set up a known query state and verify both
        // gauges return expected values — simulating what JMX would read.
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("q1", WAITING_FOR_RESOURCES, 7500),
                createQueryInfo("q2", WAITING_FOR_RESOURCES, 3200),
                createQueryInfo("q3", RUNNING, 0),
                createQueryInfo("q4", WAITING_FOR_RESOURCES, 1100));

        AdmissionStateMonitor monitor = createMonitor(() -> queries);

        // Req 7.1: WaitingQueryCount = number of queries in WAITING_FOR_RESOURCES
        assertThat(monitor.getWaitingQueryCount())
                .as("Should count 3 queries in WAITING_FOR_RESOURCES")
                .isEqualTo(3);

        // Req 7.2: LongestWaitingQueryDurationSeconds = max resourceWaitingTime
        assertThat(monitor.getLongestWaitingQueryDurationSeconds())
                .as("Should report longest waiting duration of 7.5 seconds")
                .isEqualTo(7.5);
    }

    @Test
    void testGaugeZeroWhenNoWaitingQueries()
            throws Exception
    {
        // Req 7.3: Both gauges report 0 when no query is in WAITING_FOR_RESOURCES
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("q1", RUNNING, 5000));

        AdmissionStateMonitor monitor = createMonitor(() -> queries);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugeZeroWhenEmptyQueryList()
            throws Exception
    {
        // Req 7.3: Both gauges report 0 when no queries exist at all
        AdmissionStateMonitor monitor = createMonitor(ImmutableList::of);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testMonitorClassIsPublic()
    {
        // JMX export via weakref.jmx requires the class to be public
        assertThat(java.lang.reflect.Modifier.isPublic(AdmissionStateMonitor.class.getModifiers()))
                .as("AdmissionStateMonitor must be public for JMX export")
                .isTrue();
    }

    @Test
    void testMonitorHasInjectableConstructor()
    {
        // Verify the @Inject constructor exists for Guice binding
        boolean hasInjectConstructor = java.util.Arrays.stream(AdmissionStateMonitor.class.getDeclaredConstructors())
                .anyMatch(c -> c.isAnnotationPresent(com.google.inject.Inject.class));
        assertThat(hasInjectConstructor)
                .as("AdmissionStateMonitor should have an @Inject constructor for Guice binding")
                .isTrue();
    }

    private static AdmissionStateMonitor createMonitor(Supplier<List<BasicQueryInfo>> querySupplier)
            throws Exception
    {
        sun.misc.Unsafe unsafe = getUnsafe();
        AdmissionStateMonitor monitor = (AdmissionStateMonitor) unsafe.allocateInstance(AdmissionStateMonitor.class);

        // Set ticker field
        Field tickerField = AdmissionStateMonitor.class.getDeclaredField("ticker");
        tickerField.setAccessible(true);
        tickerField.set(monitor, Ticker.systemTicker());

        // Create TestableQueryManager and set the supplier
        QueryManager queryManager = (QueryManager) unsafe.allocateInstance(TestAdmissionStateMonitor.TestableQueryManager.class);
        Field supplierField = TestAdmissionStateMonitor.TestableQueryManager.class.getDeclaredField("querySupplier");
        supplierField.setAccessible(true);
        supplierField.set(queryManager, querySupplier);

        // Set queryManager field
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
