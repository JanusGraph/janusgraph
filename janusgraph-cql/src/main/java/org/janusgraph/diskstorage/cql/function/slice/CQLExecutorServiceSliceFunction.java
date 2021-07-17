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

package org.janusgraph.diskstorage.cql.function.slice;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import io.vavr.concurrent.Future;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class CQLExecutorServiceSliceFunction extends AbstractCQLSliceFunction{

    private final CQLColValGetter getter;
    private final ExecutorService executorService;

    public CQLExecutorServiceSliceFunction(CqlSession session, PreparedStatement getSlice,
                                           CQLColValGetter getter, ExecutorService executorService) {
        super(session, getSlice);
        this.getter = getter;
        this.executorService = executorService;
    }

    @Override
    protected EntryList getSlice(CompletableFuture<AsyncResultSet> completableFutureSlice) throws BackendException {
        final Future<EntryList> result = Future.fromJavaFuture(
            this.executorService,
            completableFutureSlice
        ).map(resultSet -> fromResultSet(resultSet, this.getter));
        interruptibleWait(result);
        return result.getValue().get().getOrElseThrow(CQLKeyColumnValueStore.EXCEPTION_MAPPER);
    }

    /**
     * VAVR Future.await will throw InterruptedException wrapped in a FatalException. If the Thread was in Object.wait, the interrupted
     * flag will be cleared as a side effect and needs to be reset. This method checks that the underlying cause of the FatalException is
     * InterruptedException and resets the interrupted flag.
     *
     * @param result the future to wait on
     * @throws PermanentBackendException if the thread was interrupted while waiting for the future result
     */
    private void interruptibleWait(final Future<?> result) throws PermanentBackendException {
        try {
            result.await();
        } catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PermanentBackendException(e);
        }
    }
}
