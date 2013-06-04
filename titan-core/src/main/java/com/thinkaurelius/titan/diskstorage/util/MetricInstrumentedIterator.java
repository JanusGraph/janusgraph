package com.thinkaurelius.titan.diskstorage.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.util.stats.MetricManager;

/**
 * This class is used by {@code MetricInstrumentedStore} to measure wallclock
 * time, method invocation counts, and exceptions thrown by the methods on
 * {@link RecordIterator} instances returned from
 * {@link MetricInstrumentedStore#getSlice(com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)}.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 * 
 * @param <E> Iterator element type
 */
public class MetricInstrumentedIterator<E> implements RecordIterator<E> {
    
    private final RecordIterator<E> i;
    
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
     * @param i
     *            The iterator to wrap with Metrics measurements
     * @param p
     *            The Metrics name prefix string
     * @return A wrapper around {@code i} or null if {@code i} is null
     */
    public static <E> MetricInstrumentedIterator<E> of(RecordIterator<E> i, String p) {
        if (null == i) {
            return null;
        }
        
        Preconditions.checkNotNull(i);
        Preconditions.checkNotNull(p);
        
        return new MetricInstrumentedIterator<E>(i, p);
    }
    
    private MetricInstrumentedIterator(RecordIterator<E> i, String p) {
        this.i = i;
        
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
    public boolean hasNext() throws StorageException {
        boolean ok = false;
        hasNextInvocationCounter.inc();
        final Timer.Context tc = hasNextTimer.time();
        try {
            boolean result = i.hasNext();
            ok = true;
            return result;
        } finally {
            tc.stop();
            if (!ok) hasNextFailureCounter.inc();
        }
    }

    @Override
    public E next() throws StorageException {
        boolean ok = false;
        nextInvocationCounter.inc();
        final Timer.Context tc = nextTimer.time();
        try {
            E result = i.next();
            ok = true;
            return result;
        } finally {
            tc.stop();
            if (!ok) nextFailureCounter.inc();
        }
    }

    @Override
    public void close() throws StorageException {
        boolean ok = false;
        closeInvocationCounter.inc();
        final Timer.Context tc = closeTimer.time();
        try {
            i.close();
            ok = true;
        } finally {
            tc.stop();
            if (!ok) closeFailureCounter.inc();
        }
    }
}
