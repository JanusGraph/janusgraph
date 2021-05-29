// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.query;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.util.stats.MetricManager;

import java.util.Iterator;

/**
 * Wraps a {@link QueryExecutor} to gather metrics on the query execution and forward them to METRICS.
 *
 * @author Dan LaRocque (dan@thinkaurelius.com)
 */
public class MetricsQueryExecutor<Q extends ElementQuery,R extends JanusGraphElement,B extends BackendQuery> implements QueryExecutor<Q,R,B> {

    private final QueryExecutor<Q,R,B> qe;
    private final String metricsPrefix;
    private static final String M_CALLS = "calls";
    private static final String M_TIME  = "time";
    private static final String M_EXCEPTIONS = "exceptions";

    public MetricsQueryExecutor(String prefix, String name, QueryExecutor<Q, R, B> qe) {
        super();
        this.qe = qe;
        this.metricsPrefix = prefix + ".query." + name;
    }

    @Override
    public Iterator<R> getNew(final Q query) {
        return runWithMetrics("getNew", v -> qe.getNew(query));
    }

    @Override
    public boolean hasDeletions(final Q query) {
        return runWithMetrics("hasDeletions", v -> qe.hasDeletions(query));
    }

    @Override
    public boolean isDeleted(final Q query, final R result) {
        return runWithMetrics("isDeleted", v -> qe.isDeleted(query, result));
    }

    @Override
    public Iterator<R> execute(final Q query, final B subquery, final Object executionInfo, final QueryProfiler profiler) {
        return runWithMetrics("execute", v -> qe.execute(query, subquery, executionInfo, profiler));
    }

    private <T> T runWithMetrics(String opName, Function<Void,T> impl) {

        Preconditions.checkNotNull(opName);
        Preconditions.checkNotNull(impl);

        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(metricsPrefix, opName, M_CALLS).inc();

        try (final Timer.Context tc = mgr.getTimer(metricsPrefix, opName, M_TIME).time()) {
            return impl.apply(null);
        } catch (RuntimeException e) {
            mgr.getCounter(metricsPrefix, opName, M_EXCEPTIONS).inc();
            throw e;
        }
    }
}
