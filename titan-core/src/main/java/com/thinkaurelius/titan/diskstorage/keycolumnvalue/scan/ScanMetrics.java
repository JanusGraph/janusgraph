package com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan;

/**
 * Counters associated with a {@link ScanJob}.
 * <p>
 * Conceptually, this interface contains two separate stores of counters:
 * <ul>
 *     <li>the standard store, accessed via {@code get} and {@code increment}</li>
 *     <li>the custom store, accessed via methods with {@code custom} in their names</li>
 * </ul>
 * All counters values automatically start at zero.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanMetrics {

    /**
     * An enum of standard counters.  A value of this enum is the only parameter
     * accepted by {@link #get(com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}
     * and {@link #increment(com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     */
    public enum Metric { FAILURE, SUCCESS }

    /**
     * Get the value of a custom counter.  Only the effects of prior calls to
     * {@link #incrementCustom(String)} and {@link #incrementCustom(String, long)}
     * should be observable through this method, never the effects of prior calls to
     * {@link #increment(com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     *
     * @param metric
     * @return
     */
    public long getCustom(String metric);

    /**
     * Increment a custom counter by {@code delta}.  The effects of calls
     * to method should only be observable through {@link #getCustom(String)},
     * never through {@link #get(com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     *
     * @param metric the name of the counter
     * @param delta the amount to add to the counter
     */
    public void incrementCustom(String metric, long delta);

    /**
     * Like {@link #incrementCustom(String, long)}, except the {@code delta} is 1.
     *
     * @param metric the name of the counter to increment by 1
     */
    public void incrementCustom(String metric);

    /**
     * Get the value of a standard counter.
     *
     * @param metric the standard counter whose value should be returned
     * @return the value of the standard counter
     */
    public long get(Metric metric);

    /**
     * Increment a standard counter by 1.
     *
     * @param metric the standard counter whose value will be increased by 1
     */
    public void increment(Metric metric);

}
