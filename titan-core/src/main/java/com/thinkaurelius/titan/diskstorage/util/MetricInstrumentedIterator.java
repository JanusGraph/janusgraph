package com.thinkaurelius.titan.diskstorage.util;

import java.io.IOException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.util.stats.MetricManager;

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
    
    private final Timer nextTimer;
    private final Counter nextInvocationCounter;
    private final Counter nextFailureCounter;
    
    private final Timer hasNextTimer;
    private final Counter hasNextInvocationCounter;
    private final Counter hasNextFailureCounter;

    private final Timer closeTimer;
    private final Counter closeInvocationCounter;
    private final Counter closeFailureCounter;
    
    /**
     * If the iterator argument is non-null, then return a new
     * {@code MetricInstrumentedIterator} wrapping it. Metrics for method calls
     * on the wrapped instance will be prefixed with the string {@code p} which
     * must be non-null. If the iterator argument is null, then return null.
     * 
     * @param keyIterator
     *            The iterator to wrap with Metrics measurements
     * @param prefix
     *            The Metrics name prefix string
     *
     * @return A wrapper around {@code i} or null if {@code i} is null
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
        
        MetricRegistry metrics = MetricManager.INSTANCE.getRegistry();
        nextTimer =
                metrics.timer(MetricRegistry.name(p, "next", "time"));
        nextInvocationCounter =
              metrics.counter(MetricRegistry.name(p, "next", "calls"));
        nextFailureCounter =
              metrics.counter(MetricRegistry.name(p, "next", "exceptions"));
        
        hasNextTimer =
                metrics.timer(MetricRegistry.name(p, "hasNext", "time"));
        hasNextInvocationCounter =
              metrics.counter(MetricRegistry.name(p, "hasNext", "calls"));
        hasNextFailureCounter =
              metrics.counter(MetricRegistry.name(p, "hasNext", "exceptions"));
        
        closeTimer =
                metrics.timer(MetricRegistry.name(p, "close", "time"));
        closeInvocationCounter =
              metrics.counter(MetricRegistry.name(p, "close", "calls"));
        closeFailureCounter =
              metrics.counter(MetricRegistry.name(p, "close", "exceptions"));
    }

    @Override
    public boolean hasNext() {
        hasNextInvocationCounter.inc();
        Timer.Context tc = hasNextTimer.time();

        try {
            return iterator.hasNext();
        } catch (RuntimeException e) {
            hasNextFailureCounter.inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    @Override
    public StaticBuffer next() {
        nextInvocationCounter.inc();
        Timer.Context tc = nextTimer.time();

        try {
            return iterator.next();
        } catch (RuntimeException e) {
            nextFailureCounter.inc();
            throw e;
        } finally {
            tc.stop();
        }
    }

    @Override
    public void close() throws IOException {
        closeInvocationCounter.inc();
        Timer.Context tc = closeTimer.time();

        try {
            iterator.close();
        } catch (RuntimeException e) {
            closeFailureCounter.inc();
            throw e;
        } finally {
            tc.stop();
        }
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
