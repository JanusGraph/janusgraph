package com.thinkaurelius.titan.hadoop.scan;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class HadoopContextScanMetrics implements ScanMetrics {

    private final TaskInputOutputContext taskIOCtx;
    // TODO make these configurable (?)
    public static final String CUSTOM_COUNTER_GROUP = "ScanJob.Custom";
    public static final String STANDARD_COUNTER_GROUP = "ScanJob.Standard";

    public HadoopContextScanMetrics(TaskInputOutputContext taskIOCtx) {
        this.taskIOCtx = taskIOCtx;
    }

    @Override
    public long getCustom(String metric) {
        return DEFAULT_COMPAT.getContextCounter(taskIOCtx, CUSTOM_COUNTER_GROUP, metric);
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        DEFAULT_COMPAT.incrementContextCounter(taskIOCtx, CUSTOM_COUNTER_GROUP, metric, delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1L);
    }

    @Override
    public long get(Metric metric) {
        return DEFAULT_COMPAT.getContextCounter(taskIOCtx, STANDARD_COUNTER_GROUP, metric.name());
    }

    @Override
    public void increment(Metric metric) {
        DEFAULT_COMPAT.incrementContextCounter(taskIOCtx, STANDARD_COUNTER_GROUP, metric.name(), 1L);
    }
}
