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

package org.janusgraph.diskstorage.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.util.stats.MetricManager;

/**
 * This class instruments an arbitrary KeyColumnValueStore backend with Metrics.
 * The cumulative runtime of, number of invocations of, and number of exceptions
 * thrown by each interface method are instrumented with Metrics (using Timer,
 * Counter, and Counter again, respectively). The Metric names are generated by
 * calling {@link MetricRegistry#name(Class, String...)},
 * where methodName is the exact name of the method including capitalization,
 * and identifier is "time", "calls", or "exceptions".
 * <p/>
 * In addition to the three standard metrics, {@code getSlice} and
 * {@code getKeys} have some additional metrics related to their return values.
 * {@code getSlice} carries metrics with the identifiers "entries-returned" and
 * "entries-histogram". The first is a counter of total Entry objects returned.
 * The second is a histogram of the size of Entry lists returned.
 * {@code getKeys} returns a {@link RecordIterator} that manages metrics for its
 * methods.
 * <p/>
 * This implementation does not catch any exceptions. Exceptions emitted by the
 * backend store implementation are guaranteed to pass through this
 * implementation's methods.
 * <p/>
 * The implementation includes repeated {@code try...catch} boilerplate that
 * could be reduced by using reflection to determine the method name and by
 * delegating Metrics object handling to a common helper that takes a Callable
 * closure, but I'm not sure that the extra complexity and potential performance
 * hit is worth it.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class MetricInstrumentedStore implements KeyColumnValueStore {

    private final KeyColumnValueStore backend;

    private static final Logger log =
            LoggerFactory.getLogger(MetricInstrumentedStore.class);

    public static final String M_CONTAINS_KEY = "containsKey";
    public static final String M_GET_SLICE = "getSlice";
    public static final String M_MUTATE = "mutate";
    public static final String M_ACQUIRE_LOCK = "acquireLock";
    public static final String M_GET_KEYS = "getKeys";
    public static final String M_GET_PART = "getLocalKeyPartition";
    public static final String M_CLOSE = "close";

    public static final List<String> OPERATION_NAMES =
            ImmutableList.of(M_CONTAINS_KEY,M_GET_SLICE,M_MUTATE,M_ACQUIRE_LOCK,M_GET_KEYS);

    public static final String M_CALLS = "calls";
    public static final String M_TIME = "time";
    public static final String M_EXCEPTIONS = "exceptions";
    public static final String M_ENTRIES_COUNT = "entries-returned";
    public static final String M_ENTRIES_HISTO = "entries-histogram";

    public static final List<String> EVENT_NAMES =
            ImmutableList.of(M_CALLS,M_TIME,M_EXCEPTIONS,M_ENTRIES_COUNT,M_ENTRIES_HISTO);

    public static final String M_ITERATOR = "iterator";

    private final String metricsStoreName;

    public MetricInstrumentedStore(KeyColumnValueStore backend, String metricsStoreName) {
        this.backend = backend;
        this.metricsStoreName = metricsStoreName;
        log.debug("Wrapped Metrics named \"{}\" around store {}", metricsStoreName, backend);
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        return runWithMetrics(txh, metricsStoreName, M_GET_SLICE,
            new StorageCallable<EntryList>() {
                public EntryList call() throws BackendException {
                    EntryList result = backend.getSlice(query, txh);
                    recordSliceMetrics(txh, result);
                    return result;
                }
            }
        );
    }

    @Override
    public Map<StaticBuffer,EntryList> getSlice(final List<StaticBuffer> keys,
                                      final SliceQuery query,
                                      final StoreTransaction txh) throws BackendException {
        return runWithMetrics(txh, metricsStoreName, M_GET_SLICE,
            new StorageCallable<Map<StaticBuffer,EntryList>>() {
                public Map<StaticBuffer,EntryList> call() throws BackendException {
                    Map<StaticBuffer,EntryList> results = backend.getSlice(keys, query, txh);

                    for (EntryList result : results.values()) {
                        recordSliceMetrics(txh, result);
                    }
                    return results;
                }
            }
        );
    }

    @Override
    public void mutate(final StaticBuffer key,
                       final List<Entry> additions,
                       final List<StaticBuffer> deletions,
                       final StoreTransaction txh) throws BackendException {
        runWithMetrics(txh, metricsStoreName, M_MUTATE,
                new StorageCallable<Void>() {
                    public Void call() throws BackendException {
                        backend.mutate(key, additions, deletions, txh);
                        return null;
                    }
                }
        );
    }

    @Override
    public void acquireLock(final StaticBuffer key,
                            final StaticBuffer column,
                            final StaticBuffer expectedValue,
                            final StoreTransaction txh) throws BackendException {
        runWithMetrics(txh, metricsStoreName, M_ACQUIRE_LOCK,
            new StorageCallable<Void>() {
                public Void call() throws BackendException {
                    backend.acquireLock(key, column, expectedValue, txh);
                    return null;
                }
            }
        );
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        return runWithMetrics(txh, metricsStoreName, M_GET_KEYS,
            new StorageCallable<KeyIterator>() {
                public KeyIterator call() throws BackendException {
                    KeyIterator ki = backend.getKeys(query, txh);
                    if (txh.getConfiguration().hasGroupName()) {
                        return MetricInstrumentedIterator.of(ki,txh.getConfiguration().getGroupName(),metricsStoreName,M_GET_KEYS,M_ITERATOR);
                    } else {
                        return ki;
                    }
                }
            }
        );
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        return runWithMetrics(txh, metricsStoreName, M_GET_KEYS,
            new StorageCallable<KeyIterator>() {
                public KeyIterator call() throws BackendException {
                    KeyIterator ki = backend.getKeys(query, txh);
                    if (txh.getConfiguration().hasGroupName()) {
                        return MetricInstrumentedIterator.of(ki,txh.getConfiguration().getGroupName(),metricsStoreName,M_GET_KEYS,M_ITERATOR);
                    } else {
                        return ki;
                    }
                }
            }
        );
    }

    @Override
    public String getName() {
        return backend.getName();
    }

    @Override
    public void close() throws BackendException {
        backend.close();
    }

    private void recordSliceMetrics(StoreTransaction txh, List<Entry> row) {
        if (!txh.getConfiguration().hasGroupName())
            return;

        String p = txh.getConfiguration().getGroupName();
        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(p, metricsStoreName, M_GET_SLICE, M_ENTRIES_COUNT).inc(row.size());
        mgr.getHistogram(p, metricsStoreName, M_GET_SLICE, M_ENTRIES_HISTO).update(row.size());
    }

    static <T> T runWithMetrics(StoreTransaction txh, String storeName, String name, StorageCallable<T> impl) throws BackendException {

        if (!txh.getConfiguration().hasGroupName()) {
            return impl.call();
        }
        String prefix = txh.getConfiguration().getGroupName();
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(impl);

        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(prefix, storeName, name, M_CALLS).inc();
        final Timer.Context tc = mgr.getTimer(prefix, storeName, name, M_TIME).time();

        try {
            return impl.call();
        } catch (BackendException e) {
            mgr.getCounter(prefix, storeName, name, M_EXCEPTIONS).inc();
            throw e;
        } catch (RuntimeException e) {
            mgr.getCounter(prefix, storeName, name, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    static <T> T runWithMetrics(String prefix, String storeName, String name, IOCallable<T> impl) throws IOException {

        if (null == prefix) {
            return impl.call();
        }

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(impl);

        final MetricManager mgr = MetricManager.INSTANCE;
        mgr.getCounter(prefix, storeName, name, M_CALLS).inc();
        final Timer.Context tc = mgr.getTimer(prefix, storeName, name, M_TIME).time();

        try {
            return impl.call();
        } catch (IOException e) {
            mgr.getCounter(prefix, storeName, name, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    static <T> T runWithMetrics(String prefix, String storeName, String name, UncheckedCallable<T> impl) {

        if (null == prefix) {
            return impl.call();
        }

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(impl);

        final MetricManager mgr = MetricManager.INSTANCE;

        mgr.getCounter(prefix, storeName, name, M_CALLS).inc();

        final Timer.Context tc = mgr.getTimer(prefix, storeName, name, M_TIME).time();

        try {
            return impl.call();
        } catch (RuntimeException e) {
            mgr.getCounter(prefix, storeName, name, M_EXCEPTIONS).inc();
            throw e;
        } finally {
            tc.stop();
        }
    }
}
