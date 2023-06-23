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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.ChunkedJobDefinition;
import org.janusgraph.diskstorage.util.EntryListComputationContext;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

public abstract class AsyncCQLFunction<Q> implements CQLSliceFunction<Q>{

    private static final Function<Row, Tuple3<StaticBuffer, StaticBuffer, Row>> ROW_TUPLE_3_FUNCTION = row -> row == null
        ? null
        : Tuple.of(StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.COLUMN_COLUMN_NAME)),
        StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.VALUE_COLUMN_NAME)), row);

    private final CqlSession session;
    private final PreparedStatement getSlice;
    private final CQLColValGetter getter;
    private final ExecutorService executorService;
    private final QueryBackPressure queryBackPressure;

    public AsyncCQLFunction(CqlSession session, PreparedStatement getSlice,
                            CQLColValGetter getter, ExecutorService executorService, QueryBackPressure queryBackPressure) {
        this.session = session;
        this.getSlice = getSlice;
        this.getter = getter;
        this.executorService = executorService;
        this.queryBackPressure = queryBackPressure;
    }

    @Override
    public CompletableFuture<EntryList> execute(Q query, StoreTransaction txh) {

        ChunkedJobDefinition<Iterator<Tuple3<StaticBuffer, StaticBuffer, Row>>, EntryListComputationContext, EntryList> chunkedJobDefinition = new ChunkedJobDefinition<>();

        queryBackPressure.acquireBeforeQuery();

        try{
            this.session.executeAsync(bindMarkers(query, this.getSlice.boundStatementBuilder())
                    .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())
                .whenComplete((asyncResultSet, throwable) -> acceptDataChunk(asyncResultSet, throwable, chunkedJobDefinition));
        } catch (RuntimeException e){
            queryBackPressure.releaseAfterQuery();
            throw e;
        }

        return chunkedJobDefinition.getResult();
    }

    abstract BoundStatementBuilder bindMarkers(Q query, BoundStatementBuilder statementBuilder);

    /**
     * This method must be non-blocking because it's executed in CQL IO thread.
     * Any computation heavy operation must be executed via `executorService`.
     */
    private void acceptDataChunk(final AsyncResultSet resultSet, final Throwable exception,
                                 final ChunkedJobDefinition<Iterator<Tuple3<StaticBuffer, StaticBuffer, Row>>, EntryListComputationContext, EntryList> chunkedJobDefinition) {

        if(exception != null){
            queryBackPressure.releaseAfterQuery();
            chunkedJobDefinition.getResult().completeExceptionally(exception);
            return;
        }

        if(chunkedJobDefinition.getResult().isCompletedExceptionally()){
            queryBackPressure.releaseAfterQuery();
            return;
        }

        try{

            chunkedJobDefinition.getDataChunks().add(Iterators.transform(resultSet.currentPage().iterator(), ROW_TUPLE_3_FUNCTION));

            if(resultSet.hasMorePages()){
                resultSet.fetchNextPage().whenComplete((asyncResultSet, throwable) -> acceptDataChunk(asyncResultSet, throwable, chunkedJobDefinition));
            } else {
                chunkedJobDefinition.setLastChunkRetrieved();
                queryBackPressure.releaseAfterQuery();
            }

        } catch (RuntimeException e){
            queryBackPressure.releaseAfterQuery();
            chunkedJobDefinition.getResult().completeExceptionally(e);
            throw e;
        }

        StaticArrayEntryList.supplyEntryList(chunkedJobDefinition, getter, executorService);
    }

}
