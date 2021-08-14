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

package org.janusgraph.diskstorage.cql.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.configuration.ExecutorServiceConfiguration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceInstrumentation;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.cql.function.mutate.CQLExecutorServiceMutateManyLoggedFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLExecutorServiceMutateManyUnloggedFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLSimpleMutateManyLoggedFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLSimpleMutateManyUnloggedFunction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_CORE_POOL_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_KEEP_ALIVE_TIME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_MAX_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;

public class CQLMutateManyFunctionBuilder {

    public CQLMutateManyFunctionWrapper build(final CqlSession session, final Configuration configuration,
                                               final TimestampProvider times, final boolean assignTimestamp,
                                               final Map<String, CQLKeyColumnValueStore> openStores,
                                               ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction){

        ExecutorService executorService;
        CQLMutateManyFunction mutateManyFunction;

        int batchSize = configuration.get(BATCH_STATEMENT_SIZE);
        boolean atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);

        if(configuration.get(EXECUTOR_SERVICE_ENABLED)){
            executorService = buildExecutorService(configuration);
            try{
                if(atomicBatch){
                    mutateManyFunction = new CQLExecutorServiceMutateManyLoggedFunction(times,
                        assignTimestamp, openStores, session, executorService, sleepAfterWriteFunction);
                } else {
                    mutateManyFunction = new CQLExecutorServiceMutateManyUnloggedFunction(batchSize,
                        session, openStores, times, executorService, assignTimestamp, sleepAfterWriteFunction);
                }
            } catch (RuntimeException e){
                executorService.shutdown();
                throw e;
            }
        } else {
            executorService = null;
            if(atomicBatch){
                mutateManyFunction = new CQLSimpleMutateManyLoggedFunction(times,
                    assignTimestamp, openStores, session, sleepAfterWriteFunction);
            } else {
                mutateManyFunction = new CQLSimpleMutateManyUnloggedFunction(batchSize,
                    session, openStores, times, assignTimestamp, sleepAfterWriteFunction);
            }
        }

        return new CQLMutateManyFunctionWrapper(executorService, mutateManyFunction);
    }

    private ExecutorService buildExecutorService(Configuration configuration) {
        Integer corePoolSize = configuration.getOrDefault(EXECUTOR_SERVICE_CORE_POOL_SIZE);
        Integer maxPoolSize = configuration.getOrDefault(EXECUTOR_SERVICE_MAX_POOL_SIZE);
        Long keepAliveTime = configuration.getOrDefault(EXECUTOR_SERVICE_KEEP_ALIVE_TIME);
        String executorServiceClass = configuration.getOrDefault(EXECUTOR_SERVICE_CLASS);
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("CQLStoreManager[%02d]")
            .build();
        if (configuration.get(BASIC_METRICS)) {
            threadFactory = ExecutorServiceInstrumentation.instrument(configuration.get(METRICS_PREFIX), "CqlStoreManager", threadFactory);
        }

        ExecutorServiceConfiguration executorServiceConfiguration =
            new ExecutorServiceConfiguration(executorServiceClass, corePoolSize, maxPoolSize, keepAliveTime, threadFactory);
        ExecutorService executorService = ExecutorServiceBuilder.build(executorServiceConfiguration);
        if (configuration.get(BASIC_METRICS)) {
            executorService = ExecutorServiceInstrumentation.instrument(configuration.get(METRICS_PREFIX), "CqlStoreManager", executorService);
        }
        return executorService;
    }

}
