package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanMetrics {

    public enum Metric { FAILURE, SUCCESS }

    public long getCustom(String metric);

    public void incrementCustom(String metric, long delta);

    public void incrementCustom(String metric);

    public long get(Metric metric);

    public void increment(Metric metric);

}
