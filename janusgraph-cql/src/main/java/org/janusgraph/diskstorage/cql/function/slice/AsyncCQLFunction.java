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
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLRowGetter;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.ChunkedJobDefinition;
import org.janusgraph.diskstorage.util.EntryListComputationContext;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

public abstract class AsyncCQLFunction<Q> implements CQLSliceFunction<Q>{

    private final CqlSession session;
    private final PreparedStatement getSlice;
    private final EntryMetaData[] schema;
    private final ExecutorService executorService;
    private final QueryBackPressure queryBackPressure;
    private final int maxInlineRows;
    /**
     * Resolved from the columns of the first result, which every result of the prepared statement shares. Two threads
     * may resolve it at once and both keep their own, equal, instance.
     */
    private volatile CQLRowGetter getter;

    /**
     * @param maxInlineRows the most rows a result may have to be turned into entries on the driver's I/O thread, see
     *                      {@link #acceptFirstPage(AsyncResultSet, Throwable, CompletableFuture)}
     */
    public AsyncCQLFunction(CqlSession session, PreparedStatement getSlice, CQLColValGetter colValGetter,
                            ExecutorService executorService, QueryBackPressure queryBackPressure, int maxInlineRows) {
        this.session = session;
        this.getSlice = getSlice;
        this.schema = colValGetter.getSchema();
        this.executorService = executorService;
        this.queryBackPressure = queryBackPressure;
        this.maxInlineRows = maxInlineRows;
    }

    @Override
    public CompletableFuture<EntryList> execute(Q query, StoreTransaction txh) {

        final CompletableFuture<EntryList> result = new CompletableFuture<>();

        queryBackPressure.acquireBeforeQuery();

        try{
            this.session.executeAsync(bindMarkers(query, this.getSlice.boundStatementBuilder())
                    .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()).build())
                .whenComplete((asyncResultSet, throwable) -> acceptFirstPage(asyncResultSet, throwable, result));
        } catch (RuntimeException e){
            queryBackPressure.releaseAfterQuery();
            throw e;
        }

        return result;
    }

    abstract BoundStatementBuilder bindMarkers(Q query, BoundStatementBuilder statementBuilder);

    /**
     * Runs on an I/O thread of the driver, so it must not block. A result which is one page of at most
     * {@code maxInlineRows} rows is turned into entries right here, copied once into arrays of the exact size, which
     * spares the hand-off to the executor service and the thread wake-up it costs the reader waiting for the result.
     * A larger result, and every result of several pages, is handed to the executor service page by page, as the
     * driver's threads have to stay free for the other requests they serve.
     */
    private void acceptFirstPage(final AsyncResultSet resultSet, final Throwable exception, final CompletableFuture<EntryList> result) {

        if(exception != null){
            queryBackPressure.releaseAfterQuery();
            result.completeExceptionally(exception);
            return;
        }

        final CQLRowGetter getter;
        try{
            getter = getter(resultSet);
        } catch (Throwable e){
            queryBackPressure.releaseAfterQuery();
            result.completeExceptionally(e);
            return;
        }

        if(!resultSet.hasMorePages() && resultSet.remaining() <= maxInlineRows){
            queryBackPressure.releaseAfterQuery();
            try{
                result.complete(toEntryList(resultSet, getter));
            } catch (Throwable e){
                result.completeExceptionally(e);
            }
            return;
        }

        acceptDataChunk(resultSet, null, new ChunkedJobDefinition<>(result), getter);
    }

    private CQLRowGetter getter(final AsyncResultSet resultSet) {
        CQLRowGetter getter = this.getter;
        if(getter == null){
            getter = new CQLRowGetter(schema, resultSet.getColumnDefinitions());
            this.getter = getter;
        }
        return getter;
    }

    private static EntryList toEntryList(final AsyncResultSet resultSet, final CQLRowGetter getter) {
        final int rows = resultSet.remaining();
        if(rows == 0){
            return EntryList.EMPTY_LIST;
        }
        // the page can be iterated only once, and the entries are sized before they are copied
        final Row[] page = new Row[rows];
        int i = 0;
        for (Row row : resultSet.currentPage()) {
            page[i++] = row;
        }
        return StaticArrayEntryList.ofByteBuffer(Arrays.asList(page), getter);
    }

    /**
     * This method must be non-blocking because it's executed in CQL IO thread.
     * Any computation heavy operation must be executed via `executorService`.
     */
    private void acceptDataChunk(final AsyncResultSet resultSet, final Throwable exception,
                                 final ChunkedJobDefinition<Iterator<Row>, EntryListComputationContext, EntryList> chunkedJobDefinition,
                                 final CQLRowGetter getter) {

        if(exception != null){
            queryBackPressure.releaseAfterQuery();
            chunkedJobDefinition.getResult().completeExceptionally(exception);
            return;
        }

        if(chunkedJobDefinition.getResult().isCompletedExceptionally()){
            queryBackPressure.releaseAfterQuery();
            return;
        }

        final boolean morePages;
        try{
            chunkedJobDefinition.getDataChunks().add(resultSet.currentPage().iterator());
            morePages = resultSet.hasMorePages();
            if(!morePages){
                chunkedJobDefinition.setLastChunkRetrieved();
                queryBackPressure.releaseAfterQuery();
            }
        } catch (RuntimeException e){
            queryBackPressure.releaseAfterQuery();
            chunkedJobDefinition.getResult().completeExceptionally(e);
            throw e;
        }

        // the page is handed to the executor service before the next one is fetched, so that a rejected hand-off,
        // as an executor service which has been shut down answers with, fetches no further page
        try{
            StaticArrayEntryList.supplyEntryListOfByteBuffer(chunkedJobDefinition, getter, executorService);
        } catch (Throwable e){
            if(morePages){
                queryBackPressure.releaseAfterQuery();
            }
            chunkedJobDefinition.getResult().completeExceptionally(e);
            return;
        }

        if(morePages){
            try{
                resultSet.fetchNextPage().whenComplete((asyncResultSet, throwable) -> acceptDataChunk(asyncResultSet, throwable, chunkedJobDefinition, getter));
            } catch (RuntimeException e){
                queryBackPressure.releaseAfterQuery();
                chunkedJobDefinition.getResult().completeExceptionally(e);
                throw e;
            }
        }
    }

}
