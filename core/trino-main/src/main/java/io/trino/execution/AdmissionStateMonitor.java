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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.inject.Inject;
import io.trino.server.BasicQueryInfo;
import org.weakref.jmx.Managed;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AdmissionStateMonitor
{
    private final QueryManager queryManager;
    private final Ticker ticker;

    @Inject
    public AdmissionStateMonitor(QueryManager queryManager)
    {
        this(queryManager, Ticker.systemTicker());
    }

    @VisibleForTesting
    AdmissionStateMonitor(QueryManager queryManager, Ticker ticker)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    @Managed
    public int getWaitingQueryCount()
    {
        List<BasicQueryInfo> queries = queryManager.getQueries();
        int count = 0;
        for (BasicQueryInfo query : queries) {
            if (query.getState() == QueryState.WAITING_FOR_RESOURCES) {
                count++;
            }
        }
        return count;
    }

    @Managed
    public double getLongestWaitingQueryDurationSeconds()
    {
        List<BasicQueryInfo> queries = queryManager.getQueries();
        long maxMillis = 0;
        for (BasicQueryInfo query : queries) {
            if (query.getState() == QueryState.WAITING_FOR_RESOURCES) {
                long millis = query.getQueryStats().getResourceWaitingTime().toMillis();
                if (millis > maxMillis) {
                    maxMillis = millis;
                }
            }
        }
        return maxMillis / 1e3;
    }
}
