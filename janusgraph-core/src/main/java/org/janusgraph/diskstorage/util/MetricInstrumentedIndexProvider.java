// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.util;

import com.codahale.metrics.Timer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.util.stats.MetricManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MetricInstrumentedIndexProvider implements IndexProvider {
    private final MetricManager metricManager = MetricManager.INSTANCE;
    private final IndexProvider indexProvider;
    private final String prefix;
    public static final String M_MUTATE = "mutate";
    public static final String M_RESTORE = "restore";
    public static final String M_QUERY = "query";
    public static final String M_MIXED_COUNT_QUERY = "mixedIndexCountQuery";
    public static final String M_RAW_QUERY = "rawQuery";
    public static final String M_TOTALS = "totals";
    public static final String M_CALLS = "calls";
    public static final String M_TIME = "time";
    public static final String M_EXCEPTIONS = "exceptions";
    public static final List<String> OPERATION_NAMES = Collections.unmodifiableList(
        Arrays.asList(M_MUTATE, M_RESTORE, M_QUERY, M_MIXED_COUNT_QUERY, M_RAW_QUERY, M_TOTALS));

    public MetricInstrumentedIndexProvider(final IndexProvider indexProvider, String prefix) {
        this.indexProvider = indexProvider;
        this.prefix = prefix;
    }

    @Override
    public void register(final String store, final String key, final KeyInformation information, final BaseTransaction tx) throws BackendException {
        indexProvider.register(store, key, information, tx);
    }

    @Override
    public void mutate(final Map<String, Map<String, IndexMutation>> mutations, final KeyInformation.IndexRetriever information,
                       final BaseTransaction tx) throws BackendException {
        runWithMetrics((BaseTransactionConfigurable) tx, M_MUTATE, () -> indexProvider.mutate(mutations, information, tx));
    }

    @Override
    public void restore(
        final Map<String, Map<String, List<IndexEntry>>> documents, final KeyInformation.IndexRetriever information,
        final BaseTransaction tx) throws BackendException {
        runWithMetrics((BaseTransactionConfigurable) tx, M_RESTORE, () -> indexProvider.restore(documents, information, tx));
    }

    @Override
    public Long queryCount(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return runWithMetrics((BaseTransactionConfigurable) tx, M_MIXED_COUNT_QUERY, () -> indexProvider.queryCount(query, information, tx));
    }

    @Override
    public Stream<String> query(final IndexQuery query, final KeyInformation.IndexRetriever information,
                                final BaseTransaction tx) throws BackendException {
        return runWithMetrics((BaseTransactionConfigurable) tx, M_QUERY, () -> indexProvider.query(query, information, tx));
    }

    @Override
    public Stream<RawQuery.Result<String>> query(final RawQuery query, final KeyInformation.IndexRetriever information,
                                                 final BaseTransaction tx) throws BackendException {
        return runWithMetrics((BaseTransactionConfigurable) tx, M_RAW_QUERY, () -> indexProvider.query(query, information, tx));
    }

    @Override
    public Long totals(final RawQuery query, final KeyInformation.IndexRetriever information, final BaseTransaction tx) throws BackendException {
        return runWithMetrics((BaseTransactionConfigurable) tx, M_TOTALS, () -> indexProvider.totals(query, information, tx));
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return indexProvider.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        indexProvider.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        indexProvider.clearStorage();
    }

    @Override
    public boolean exists() throws BackendException {
        return indexProvider.exists();
    }

    @Override
    public boolean supports(final KeyInformation information, final JanusGraphPredicate janusgraphPredicate) {
        return indexProvider.supports(information, janusgraphPredicate);
    }

    @Override
    public boolean supports(final KeyInformation information) {
        return indexProvider.supports(information);
    }

    @Override
    public String mapKey2Field(final String key, final KeyInformation information) {
        return indexProvider.mapKey2Field(key, information);
    }

    @Override
    public IndexFeatures getFeatures() {
        return indexProvider.getFeatures();
    }

    private void runWithMetrics(BaseTransactionConfigurable tx, String name, StorageRunnable impl) throws BackendException {
        if (!tx.getConfiguration().hasGroupName()) {
            impl.run();
        }

        String groupName = tx.getConfiguration().getGroupName();
        Timer.Context tc = incrementCallsAndReturnTimerContext(groupName, name);
        try {
            impl.run();
        } catch (RuntimeException | BackendException e) {
            incrementExceptions(groupName, name);
            throw e;
        } finally {
            tc.stop();
        }
    }

    private <T> T runWithMetrics(BaseTransactionConfigurable tx, String name, StorageCallable<T> impl) throws BackendException {
        if (!tx.getConfiguration().hasGroupName()) {
            return impl.call();
        }

        String groupName = tx.getConfiguration().getGroupName();
        Timer.Context tc = incrementCallsAndReturnTimerContext(groupName, name);
        try {
            return impl.call();
        } catch (RuntimeException | BackendException e) {
            incrementExceptions(groupName, name);
            throw e;
        } finally {
            tc.stop();
        }
    }

    private Timer.Context incrementCallsAndReturnTimerContext(String groupName, String name) {
        metricManager.getCounter(groupName, prefix, name, M_CALLS).inc();
        return metricManager.getTimer(groupName, prefix, name, M_TIME).time();
    }

    private void incrementExceptions(String groupName, String name) {
        metricManager.getCounter(groupName, prefix, name, M_EXCEPTIONS).inc();
    }
}
