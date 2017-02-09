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

package org.janusgraph.hadoop.scan;

import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import static org.janusgraph.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

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
