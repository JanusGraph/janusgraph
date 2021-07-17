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
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class CQLExecutorServiceMutateManyUnloggedFunction extends AbstractCQLMutateManyUnloggedFunction{

    private final ExecutorService executorService;

    public CQLExecutorServiceMutateManyUnloggedFunction(int batchSize, CqlSession session, Map<String, CQLKeyColumnValueStore> openStores,
                                                        TimestampProvider times, ExecutorService executorService, boolean assignTimestamp,
                                                        ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction) {
        super(times, assignTimestamp, session, openStores, batchSize, sleepAfterWriteFunction);
        this.executorService = executorService;
    }

    @Override
    protected Optional<Throwable> mutate(DistributedStoreManager.MaskedTimestamp commitTime, Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) {
        final Future<Seq<AsyncResultSet>> result = Future.sequence(this.executorService, Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            final String tableName = tableNameAndMutations.getKey();
            final Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();
            final CQLKeyColumnValueStore columnValueStore = getColumnValueStore(tableName);
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                final StaticBuffer key = keyAndMutations.getKey();
                final KCVMutation keyMutations = keyAndMutations.getValue();
                return toGroupedBatchableStatementsSequenceIterator(commitTime, keyMutations, columnValueStore, key)
                    .map(group -> Future.fromJavaFuture(this.executorService, execAsyncUnlogged(group, txh)));
            });
        }));

        result.await();
        if (result.isFailure()) {
            return Optional.of(result.getCause().get());
        }

        return Optional.empty();
    }
}
