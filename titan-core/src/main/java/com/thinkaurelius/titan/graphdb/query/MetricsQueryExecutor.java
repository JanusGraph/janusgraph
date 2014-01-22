package com.thinkaurelius.titan.graphdb.query;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.util.stats.MetricManager;

/*
 * Does not measure time spent in returned iterators
 */
public class MetricsQueryExecutor<Q extends ElementQuery,R extends TitanElement,B extends BackendQuery> implements QueryExecutor<Q,R,B> {

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
        return runWithMetrics("getNew", new Function<Void, Iterator<R>>() {
            @Override
            public Iterator<R> apply(Void v) {
                return qe.getNew(query);
            }
        });
    }

    @Override
    public boolean hasDeletions(final Q query) {
        return runWithMetrics("hasDeletions", new Function<Void, Boolean>() {
            @Override
            public Boolean apply(Void v) {
                return qe.hasDeletions(query);
            }
        });
    }

    @Override
    public boolean isDeleted(final Q query, final R result) {
        return runWithMetrics("isDeleted", new Function<Void, Boolean>() {
            @Override
            public Boolean apply(Void v) {
                return qe.isDeleted(query, result);
            }
        });
    }

    @Override
    public Iterator<R> execute(final Q query, final B subquery, final Object executionInfo) {
        return runWithMetrics("execute", new Function<Void, Iterator<R>>() {
            @Override
            public Iterator<R> apply(Void v) {
                return qe.execute(query, subquery, executionInfo);
            }
        });
    }

    private <T> T runWithMetrics(String opName, Function<Void,T> impl) {

        Preconditions.checkNotNull(opName);
        Preconditions.checkNotNull(impl);

        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(metricsPrefix, opName, M_CALLS).inc();
        final Timer.Context tc = mgr.getTimer(metricsPrefix, opName, M_TIME).time();

        try {
            return impl.apply(null);
        } catch (RuntimeException e) {
            mgr.getCounter(metricsPrefix, opName, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }
}
