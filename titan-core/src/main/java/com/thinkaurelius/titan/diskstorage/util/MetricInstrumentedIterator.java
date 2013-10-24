package com.thinkaurelius.titan.diskstorage.util;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;

/**
 * This class is used by {@code MetricInstrumentedStore} to measure wallclock
 * time, method invocation counts, and exceptions thrown by the methods on
 * {@link RecordIterator} instances returned from
 * {@link MetricInstrumentedStore#getSlice(com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)}.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class MetricInstrumentedIterator implements KeyIterator {
    
    private final KeyIterator iterator;
    private final String p;
    
    private static final String M_HAS_NEXT = "hasNext";
    private static final String M_NEXT = "next";
    private static final String M_CLOSE = "close";

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
    public static MetricInstrumentedIterator of(KeyIterator keyIterator, String prefix) {
        if (keyIterator == null) {
            return null;
        }

        Preconditions.checkNotNull(prefix);
        return new MetricInstrumentedIterator(keyIterator, prefix);
    }
    
    private MetricInstrumentedIterator(KeyIterator i, String p) {
        this.iterator = i;
        this.p = p;
    }

    @Override
    public boolean hasNext() {
        return MetricInstrumentedStore.runWithMetrics(p, null, M_HAS_NEXT,
            new UncheckedCallable<Boolean>() {
                public Boolean call() {
                    return Boolean.valueOf(iterator.hasNext());
                }
            }
        );
    }

    @Override
    public StaticBuffer next() {
        return MetricInstrumentedStore.runWithMetrics(p, null, M_NEXT,
            new UncheckedCallable<StaticBuffer>() {
                public StaticBuffer call() {
                    return iterator.next();
                }
            }
        );
    }
    
    @Override
    public void close() throws IOException {
        MetricInstrumentedStore.runWithMetrics(p, null, M_CLOSE,
            new IOCallable<Void>() {
                public Void call() throws IOException {
                    iterator.close();
                    return null;
                }
            }
        );
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
