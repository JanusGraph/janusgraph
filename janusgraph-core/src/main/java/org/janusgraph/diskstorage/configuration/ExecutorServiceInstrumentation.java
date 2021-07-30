// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import org.janusgraph.util.stats.MetricManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class ExecutorServiceInstrumentation {

    private static final String METRICS_NAMESPACE = "threadpools";
    private static final String METRICS_THREAD_FACTORY = "threadFactory";
    private static final String METRICS_EXECUTOR_SERVICE = "executorService";
    private static final String METRICS_QUEUE_SIZE = "queueSize";

    private ExecutorServiceInstrumentation() {}

    public static InstrumentedExecutorService instrument(String metricsPrefix, String name, ExecutorService executorService) {
        return instrument(metricsPrefix, name, executorService, MetricManager.INSTANCE.getRegistry());
    }

    @VisibleForTesting
    static InstrumentedExecutorService instrument(String metricsPrefix, String name, ExecutorService executorService, MetricRegistry registry) {
        if (executorService instanceof ThreadPoolExecutor) {
            registry.gauge(
                fullMetricsName(metricsPrefix, name, METRICS_EXECUTOR_SERVICE, METRICS_QUEUE_SIZE),
                () -> (Gauge<Integer>) () -> ((ThreadPoolExecutor) executorService).getQueue().size()
            );
        }
        return new InstrumentedExecutorService(
            executorService,
            registry,
            fullMetricsName(metricsPrefix, name, METRICS_EXECUTOR_SERVICE)
        );
    }

    public static InstrumentedThreadFactory instrument(String metricsPrefix, String name, ThreadFactory threadFactory) {
        return instrument(metricsPrefix, name, threadFactory, MetricManager.INSTANCE.getRegistry());
    }

    @VisibleForTesting
    static InstrumentedThreadFactory instrument(String metricsPrefix, String name, ThreadFactory threadFactory, MetricRegistry registry) {
        return new InstrumentedThreadFactory(
            threadFactory,
            registry,
            fullMetricsName(metricsPrefix, name, METRICS_THREAD_FACTORY)
        );
    }

    private static String fullMetricsName(String prefix, String... parts) {
        return prefix + "." + METRICS_NAMESPACE + "." + String.join(".", parts);
    }
}
