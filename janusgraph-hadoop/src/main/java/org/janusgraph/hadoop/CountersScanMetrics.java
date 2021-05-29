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

package org.janusgraph.hadoop;

import org.apache.hadoop.mapreduce.Counters;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.hadoop.scan.HadoopContextScanMetrics;

public class CountersScanMetrics implements ScanMetrics {

    private final Counters counters;

    public CountersScanMetrics(Counters counters) {
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
