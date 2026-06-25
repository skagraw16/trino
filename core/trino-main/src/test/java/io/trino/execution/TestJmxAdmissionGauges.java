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
import com.google.inject.Inject;
import io.trino.server.BasicQueryInfo;
import org.junit.jupiter.api.Test;
import org.weakref.jmx.Managed;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.execution.TestAdmissionStateMonitor.createQueryInfo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link AdmissionStateMonitor} is properly structured for JMX
 * export and that its gauges produce accurate values for a known query state.
 *
 * <p>This is a lightweight validation of the MBean contract without requiring a
 * full {@code DistributedQueryRunner} boot. It verifies Req 7.6 by ensuring the
 * {@code @Managed} methods are available for JMX registration.
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
    void testManagedMethodsArePublicNoArgGetters()
            throws Exception
    {
        // JMX gauge attributes must be public, parameterless getters
        Method waitingCount = AdmissionStateMonitor.class.getMethod("getWaitingQueryCount");
        Method longestDuration = AdmissionStateMonitor.class.getMethod("getLongestWaitingQueryDurationSeconds");

        assertThat(Modifier.isPublic(waitingCount.getModifiers()))
                .as("getWaitingQueryCount must be public for JMX export")
                .isTrue();
        assertThat(waitingCount.getParameterCount()).isEqualTo(0);
        assertThat(Modifier.isPublic(longestDuration.getModifiers()))
                .as("getLongestWaitingQueryDurationSeconds must be public for JMX export")
                .isTrue();
        assertThat(longestDuration.getParameterCount()).isEqualTo(0);
    }

    @Test
    void testGaugesDoNotCreateBackgroundThreads()
    {
        // Req 7.5: compute on-demand, no background threads, executors, or timers
        for (Field field : AdmissionStateMonitor.class.getDeclaredFields()) {
            assertThat(field.getType().getName())
                    .as("Field %s should not be a thread/executor type", field.getName())
                    .doesNotContain("ExecutorService")
                    .doesNotContain("ScheduledExecutor")
                    .doesNotContain("java.lang.Thread");
        }
    }

    @Test
    void testMonitorIsPublicWithInjectableConstructor()
    {
        // weakref.jmx requires the class to be public; Guice requires an @Inject constructor
        assertThat(Modifier.isPublic(AdmissionStateMonitor.class.getModifiers()))
                .as("AdmissionStateMonitor must be public for JMX export")
                .isTrue();
        assertThat(Arrays.stream(AdmissionStateMonitor.class.getDeclaredConstructors())
                .anyMatch(constructor -> constructor.isAnnotationPresent(Inject.class)))
                .as("AdmissionStateMonitor should have an @Inject constructor for Guice binding")
                .isTrue();
    }

    @Test
    void testGaugeAccuracyWithKnownState()
    {
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("q1", WAITING_FOR_RESOURCES, 7500),
                createQueryInfo("q2", WAITING_FOR_RESOURCES, 3200),
                createQueryInfo("q3", RUNNING, 0),
                createQueryInfo("q4", WAITING_FOR_RESOURCES, 1100));

        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() -> queries);

        assertThat(monitor.getWaitingQueryCount())
                .as("Should count 3 queries in WAITING_FOR_RESOURCES")
                .isEqualTo(3);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds())
                .as("Should report longest waiting duration of 7.5 seconds")
                .isEqualTo(7.5);
    }

    @Test
    void testGaugeZeroWhenNoWaitingQueries()
    {
        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() ->
                ImmutableList.of(createQueryInfo("q1", RUNNING, 5000)));

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugeZeroWhenEmptyQueryList()
    {
        AdmissionStateMonitor monitor = new AdmissionStateMonitor(ImmutableList::of);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }
}
