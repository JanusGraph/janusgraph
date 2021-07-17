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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import io.vavr.Tuple3;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;

import java.util.concurrent.CompletableFuture;

import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

public abstract class AbstractCQLSliceFunction implements CQLSliceFunction{

    private final CqlSession session;
    private final PreparedStatement getSlice;

    public AbstractCQLSliceFunction(CqlSession session, PreparedStatement getSlice) {
        this.session = session;
        this.getSlice = getSlice;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        return getSlice(
            this.session.executeAsync(this.getSlice.boundStatementBuilder()
                    .setByteBuffer(CQLKeyColumnValueStore.KEY_BINDING, query.getKey().asByteBuffer())
                    .setByteBuffer(CQLKeyColumnValueStore.SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                    .setByteBuffer(CQLKeyColumnValueStore.SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                    .setInt(CQLKeyColumnValueStore.LIMIT_BINDING, query.getLimit())
                    .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())
                .toCompletableFuture()
        );
    }

    protected static EntryList fromResultSet(final AsyncResultSet resultSet, final StaticArrayEntry.GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> getter) {
        return StaticArrayEntryList.ofStaticBuffer(new CQLKeyColumnValueStore.CQLResultSetIterator(ResultSets.newInstance(resultSet)), getter);
    }

    protected abstract EntryList getSlice(CompletableFuture<AsyncResultSet> completableFutureSlice) throws BackendException;

}
