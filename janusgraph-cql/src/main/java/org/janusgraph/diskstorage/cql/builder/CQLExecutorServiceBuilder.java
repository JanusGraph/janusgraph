// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.builder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.configuration.ExecutorServiceConfiguration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceInstrumentation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CORE_POOL_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_KEEP_ALIVE_TIME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_MAX_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;

public class CQLExecutorServiceBuilder {

    private static final AtomicLong NAME_COUNTER = new AtomicLong();

    private CQLExecutorServiceBuilder() {}

    public static ExecutorService buildExecutorService(Configuration configuration) {
        Integer corePoolSize = configuration.getOrDefault(EXECUTOR_SERVICE_CORE_POOL_SIZE);
        Integer maxPoolSize = configuration.getOrDefault(EXECUTOR_SERVICE_MAX_POOL_SIZE);
        Long keepAliveTime = configuration.getOrDefault(EXECUTOR_SERVICE_KEEP_ALIVE_TIME);
        String executorServiceClass = configuration.getOrDefault(EXECUTOR_SERVICE_CLASS);
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("CQLStoreManager[%02d]")
            .build();
        if (configuration.get(BASIC_METRICS)) {
            threadFactory = ExecutorServiceInstrumentation.instrument(
                configuration.get(METRICS_PREFIX),
                "CqlStoreManager-" + NAME_COUNTER.incrementAndGet(),
                threadFactory);
        }

        ExecutorServiceConfiguration executorServiceConfiguration =
            new ExecutorServiceConfiguration(executorServiceClass, corePoolSize, maxPoolSize, keepAliveTime, threadFactory);
        ExecutorService executorService = ExecutorServiceBuilder.build(executorServiceConfiguration);
        if (configuration.get(BASIC_METRICS)) {
            executorService = ExecutorServiceInstrumentation.instrument(
                configuration.get(METRICS_PREFIX),
                "CqlStoreManager-" + NAME_COUNTER.incrementAndGet(),
                executorService);
        }
        return executorService;
    }

}
