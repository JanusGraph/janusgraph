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
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CQLSimpleMutateManyUnloggedFunction extends AbstractCQLMutateManyUnloggedFunction{

    public CQLSimpleMutateManyUnloggedFunction(int batchSize, CqlSession session, Map<String, CQLKeyColumnValueStore> openStores,
                                               TimestampProvider times, boolean assignTimestamp,
                                               ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction) {
        super(times, assignTimestamp, session, openStores, batchSize, sleepAfterWriteFunction);
    }

    @Override
    protected Optional<Throwable> mutate(DistributedStoreManager.MaskedTimestamp commitTime, Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) {
        List<CompletableFuture<AsyncResultSet>> resultList = new LinkedList<>();

        mutations.forEach((tableName, tableMutations) -> {
            final CQLKeyColumnValueStore columnValueStore = getColumnValueStore(tableName);

            tableMutations.forEach((key, keyMutations) ->
                toGroupedBatchableStatementsSequenceIterator(commitTime, keyMutations, columnValueStore, key).forEach(group -> {
                    CompletableFuture<AsyncResultSet> completableFuture = execAsyncUnlogged(group, txh);
                    resultList.add(completableFuture);
                })
            );
        });

        for(CompletableFuture<AsyncResultSet> resultPart : resultList){
            try {
                resultPart.get();
            } catch (InterruptedException | ExecutionException e) {
                return Optional.of(e);
            }
        }

        return Optional.empty();
    }
}
