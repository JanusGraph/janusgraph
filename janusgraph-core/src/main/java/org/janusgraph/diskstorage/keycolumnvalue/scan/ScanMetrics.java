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

package org.janusgraph.diskstorage.keycolumnvalue.scan;

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
     * accepted by {@link #get(org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}
     * and {@link #increment(org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     */
    enum Metric { FAILURE, SUCCESS }

    /**
     * Get the value of a custom counter.  Only the effects of prior calls to
     * {@link #incrementCustom(String)} and {@link #incrementCustom(String, long)}
     * should be observable through this method, never the effects of prior calls to
     * {@link #increment(org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     *
     * @param metric
     * @return
     */
    long getCustom(String metric);

    /**
     * Increment a custom counter by {@code delta}.  The effects of calls
     * to method should only be observable through {@link #getCustom(String)},
     * never through {@link #get(org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics.Metric)}.
     *
     * @param metric the name of the counter
     * @param delta the amount to add to the counter
     */
    void incrementCustom(String metric, long delta);

    /**
     * Like {@link #incrementCustom(String, long)}, except the {@code delta} is 1.
     *
     * @param metric the name of the counter to increment by 1
     */
    void incrementCustom(String metric);

    /**
     * Get the value of a standard counter.
     *
     * @param metric the standard counter whose value should be returned
     * @return the value of the standard counter
     */
    long get(Metric metric);

    /**
     * Increment a standard counter by 1.
     *
     * @param metric the standard counter whose value will be increased by 1
     */
    void increment(Metric metric);

}
