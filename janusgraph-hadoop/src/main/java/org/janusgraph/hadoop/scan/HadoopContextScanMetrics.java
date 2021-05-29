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

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;

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
        return taskIOCtx.getCounter(CUSTOM_COUNTER_GROUP, metric).getValue();
    }

    @Override
    public void incrementCustom(String metric, long delta) {
        taskIOCtx.getCounter(CUSTOM_COUNTER_GROUP, metric).increment(delta);
    }

    @Override
    public void incrementCustom(String metric) {
        incrementCustom(metric, 1L);
    }

    @Override
    public long get(Metric metric) {
        return taskIOCtx.getCounter(STANDARD_COUNTER_GROUP, metric.name()).getValue();
    }

    @Override
    public void increment(Metric metric) {
        taskIOCtx.getCounter(STANDARD_COUNTER_GROUP, metric.name()).increment(1L);
    }
}
