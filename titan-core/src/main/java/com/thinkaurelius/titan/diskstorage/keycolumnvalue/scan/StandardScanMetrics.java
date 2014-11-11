package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardScanMetrics implements ScanMetrics {

    private final EnumMap<Metric,AtomicLong> metrics;
    private final ConcurrentMap<String,AtomicLong> customMetrics;

    public StandardScanMetrics() {
        metrics = new EnumMap<>(ScanMetrics.Metric.class);
        for (Metric m : Metric.values()) {
            metrics.put(m,new AtomicLong(0));
        }
        customMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public long getCustom(String metric) {
        AtomicLong counter = customMetrics.get(metric);
        if (counter == null) return 0;
        else return counter.get();
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        AtomicLong counter = customMetrics.get(metric);
        if (counter==null) {
            customMetrics.putIfAbsent(metric,new AtomicLong(0));
            counter = customMetrics.get(metric);
        }
        counter.addAndGet(delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric,1);
    }

    @Override
    public long get(Metric metric) {
        return metrics.get(metric).get();
    }

    @Override
    public void increment(Metric metric) {
        metrics.get(metric).incrementAndGet();
    }


}
