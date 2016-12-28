package org.janusgraph.hadoop.compat.h1;

import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.hadoop.scan.HadoopContextScanMetrics;
import org.apache.hadoop.mapreduce.Counters;

public class Hadoop1CountersScanMetrics implements ScanMetrics {

    private final Counters counters;

    public Hadoop1CountersScanMetrics(Counters counters) {
        this.counters = counters;
    }

    @Override
    public long getCustom(String metric) {
        return counters.getGroup(HadoopContextScanMetrics.CUSTOM_COUNTER_GROUP).findCounter(metric).getValue();
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        counters.getGroup(HadoopContextScanMetrics.CUSTOM_COUNTER_GROUP).findCounter(metric).increment(delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1L);
    }

    @Override
    public long get(Metric metric) {
        return counters.getGroup(HadoopContextScanMetrics.STANDARD_COUNTER_GROUP).findCounter(metric.name()).getValue();
    }

    @Override
    public void increment(Metric metric) {
        counters.getGroup(HadoopContextScanMetrics.STANDARD_COUNTER_GROUP).findCounter(metric.name()).increment(1L);
    }
}
