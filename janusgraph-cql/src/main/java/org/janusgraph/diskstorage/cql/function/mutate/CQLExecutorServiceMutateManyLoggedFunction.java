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

package org.janusgraph.diskstorage.cql.function.mutate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import io.vavr.concurrent.Future;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.EXCEPTION_MAPPER;

public class CQLExecutorServiceMutateManyLoggedFunction extends AbstractCQLMutateManyLoggedFunction{

    private final CqlSession session;

    private final ExecutorService executorService;

    public CQLExecutorServiceMutateManyLoggedFunction(TimestampProvider times, boolean assignTimestamp,
                                                      Map<String, CQLKeyColumnValueStore> openStores,
                                                      CqlSession session, ExecutorService executorService,
                                                      ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction) {
        super(times, assignTimestamp, openStores, sleepAfterWriteFunction);
        this.session = session;
        this.executorService = executorService;
    }

    @Override
    protected void execute(BatchStatement batchStatement) throws BackendException {
        final Future<AsyncResultSet> result = Future.fromJavaFuture(executorService,
            session.executeAsync(batchStatement).toCompletableFuture());

        result.await();

        if (result.isFailure()) {
            throw EXCEPTION_MAPPER.apply(result.getCause().get());
        }
    }
}
