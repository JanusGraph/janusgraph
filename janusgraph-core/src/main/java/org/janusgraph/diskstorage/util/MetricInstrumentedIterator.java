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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.apache.commons.lang.StringUtils;

/**
 * This class is used by {@code MetricInstrumentedStore} to measure wall clock
 * time, method invocation counts, and exceptions thrown by the methods on
 * {@link RecordIterator} instances returned from
 * {@link MetricInstrumentedStore#getSlice(org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery, org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction)}.
 * 
 * @author Dan LaRocque (dalaro@hopcount.org)
 */
public class MetricInstrumentedIterator implements KeyIterator {
    
    private final KeyIterator iterator;
    private final String p;
    
    private static final String M_HAS_NEXT = "hasNext";
    private static final String M_NEXT = "next";
    static final String M_CLOSE = "close";

    /**
     * If the iterator argument is non-null, then return a new
     * {@code MetricInstrumentedIterator} wrapping it. Metrics for method calls
     * on the wrapped instance will be prefixed with the string {@code prefix}
     * which must be non-null. If the iterator argument is null, then return
     * null.
     * 
     * @param keyIterator
     *            the iterator to wrap with Metrics measurements
     * @param prefix
     *            the Metrics name prefix string
     * 
     * @return a wrapper around {@code keyIterator} or null if
     *         {@code keyIterator} is null
     */
    public static MetricInstrumentedIterator of(KeyIterator keyIterator, String... prefix) {
        if (keyIterator == null) {
            return null;
        }

        Preconditions.checkNotNull(prefix);
        return new MetricInstrumentedIterator(keyIterator, StringUtils.join(prefix,"."));
    }
    
    private MetricInstrumentedIterator(KeyIterator i, String p) {
        this.iterator = i;
        this.p = p;
    }

    @Override
    public boolean hasNext() {
        return MetricInstrumentedStore.runWithMetrics(p, M_HAS_NEXT,
                (UncheckedCallable<Boolean>) iterator::hasNext);
    }

    @Override
    public StaticBuffer next() {
        return MetricInstrumentedStore.runWithMetrics(p, M_NEXT,
                (UncheckedCallable<StaticBuffer>) iterator::next);
    }
    
    @Override
    public void close() throws IOException {
        MetricInstrumentedStore.runWithMetrics(p, MetricInstrumentedIterator.M_CLOSE, (IOCallable<Void>) () -> {
            iterator.close();
            return null;
        });
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        // TODO: add metrics to entries if ever needed
        return iterator.getEntries();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
