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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log =
            LoggerFactory.getLogger(StandardScanMetrics.class);

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
        if (counter == null) {
            if (log.isDebugEnabled())
                log.debug("[{}:{}] Returning zero by default (was null)",
                        System.identityHashCode(customMetrics), metric);
            return 0;
        } else {
            long v = counter.get();
            if (log.isDebugEnabled())
                log.debug("[{}:{}] Returning {}", System.identityHashCode(customMetrics), metric, v);
            return v;
        }
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        AtomicLong counter = customMetrics.get(metric);
        if (counter==null) {
            customMetrics.putIfAbsent(metric,new AtomicLong(0));
            counter = customMetrics.get(metric);
        }
        counter.addAndGet(delta);
        if (log.isDebugEnabled())
            log.debug("[{}:{}] Incremented by {}", System.identityHashCode(customMetrics), metric, delta);
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
