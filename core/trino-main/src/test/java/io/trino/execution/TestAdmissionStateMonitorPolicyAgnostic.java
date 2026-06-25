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
import io.trino.server.BasicQueryInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.execution.TestAdmissionStateMonitor.createQueryInfo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts that {@link AdmissionStateMonitor} gauge readings depend only on
 * query state and not on any admission-policy state.
 *
 * <p>Since PR 1 (AdmissionPolicy SPI) has not been applied, this is trivially true:
 * the monitor class has no compile-time or runtime dependency on any admission policy
 * type. This test verifies that:
 * <ul>
 *   <li>The monitor class does not reference any admission policy classes</li>
 *   <li>Gauge readings are purely derived from the supplied query list</li>
 *   <li>Given identical query state, gauges always produce identical readings</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 7.4, 7.6</b>
 */
class TestAdmissionStateMonitorPolicyAgnostic
{
    @Test
    void testMonitorHasNoAdmissionPolicyDependency()
    {
        Class<?> monitorClass = AdmissionStateMonitor.class;

        for (Field field : monitorClass.getDeclaredFields()) {
            assertThat(field.getType().getName())
                    .as("Field %s should not reference admission policy types", field.getName())
                    .doesNotContain("admission")
                    .doesNotContain("AdmissionPolicy");
        }

        Arrays.stream(monitorClass.getDeclaredConstructors()).forEach(constructor -> {
            for (Class<?> paramType : constructor.getParameterTypes()) {
                assertThat(paramType.getName())
                        .as("Constructor parameter should not reference admission policy types")
                        .doesNotContain("admission")
                        .doesNotContain("AdmissionPolicy");
            }
        });

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
    void testGaugesArePurelyDerivedFromQueryState()
    {
        // Given a known query state, verify gauges produce the same readings
        // regardless of how many times they are called. This proves the gauges
        // are stateless and policy-agnostic.
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("query_1", WAITING_FOR_RESOURCES, 5000),
                createQueryInfo("query_2", RUNNING, 0),
                createQueryInfo("query_3", WAITING_FOR_RESOURCES, 3000),
                createQueryInfo("query_4", QUEUED, 0));

        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() -> queries);

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
    {
        AdmissionStateMonitor monitor = new AdmissionStateMonitor(ImmutableList::of);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugesWithNoWaitingQueries()
    {
        List<BasicQueryInfo> queries = ImmutableList.of(
                createQueryInfo("q1", RUNNING, 1000),
                createQueryInfo("q2", QUEUED, 500));

        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() -> queries);

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }

    @Test
    void testGaugesReflectOnlyCurrentState()
    {
        // Verify that gauges reflect only the current query list, with no hidden
        // internal state or caching. Mutate the backing list and confirm the
        // gauges immediately reflect the new state.
        List<BasicQueryInfo> queries = new ArrayList<>();
        queries.add(createQueryInfo("q1", WAITING_FOR_RESOURCES, 2000));

        AdmissionStateMonitor monitor = new AdmissionStateMonitor(() -> ImmutableList.copyOf(queries));

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(1);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(2.0);

        // "Dequeue" the query — change the backing list
        queries.clear();
        queries.add(createQueryInfo("q1", RUNNING, 0));

        assertThat(monitor.getWaitingQueryCount()).isEqualTo(0);
        assertThat(monitor.getLongestWaitingQueryDurationSeconds()).isEqualTo(0.0);
    }
}
