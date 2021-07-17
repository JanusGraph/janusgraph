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
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;

import java.util.concurrent.CompletableFuture;

public class CQLSimpleSliceFunction extends AbstractCQLSliceFunction{

    private final CQLColValGetter getter;

    public CQLSimpleSliceFunction(CqlSession session, PreparedStatement getSlice, CQLColValGetter getter) {
        super(session, getSlice);
        this.getter = getter;
    }

    @Override
    protected EntryList getSlice(CompletableFuture<AsyncResultSet> completableFutureSlice) throws BackendException {
        AsyncResultSet asyncResultSet = interruptibleWait(completableFutureSlice);
        return fromResultSet(asyncResultSet, this.getter);
    }

    private <T> T interruptibleWait(final CompletableFuture<T> result) throws BackendException {
        try {
            return result.get();
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
            throw new PermanentBackendException(e);
        } catch (Exception e) {
            throw CQLKeyColumnValueStore.EXCEPTION_MAPPER.apply(e);
        }
    }
}
