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

import io.vavr.collection.Iterator;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ColumnOperationFunction;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;
import java.util.function.Function;

public abstract class AbstractCQLMutateManyFunction {

    protected final ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction;
    protected final Function<StoreTransaction, DistributedStoreManager.MaskedTimestamp> createMaskedTimestampFunction;
    protected final ColumnOperationFunction deletionsFunction;
    protected final ColumnOperationFunction additionsFunction;

    private final Map<String, CQLKeyColumnValueStore> openStores;

    public AbstractCQLMutateManyFunction(final ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction,
                                         final boolean assignTimestamp, final TimestampProvider times, Map<String, CQLKeyColumnValueStore> openStores) {
        this.openStores = openStores;

        if(assignTimestamp){
            this.createMaskedTimestampFunction = DistributedStoreManager.MaskedTimestamp::new;
            this.sleepAfterWriteFunction = sleepAfterWriteFunction;
            this.deletionsFunction = (commitTime, keyMutations, columnValueStore, key) -> Iterator.of(commitTime.getDeletionTime(times))
                .flatMap(deleteTime -> Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deleteTime)));
            this.additionsFunction = (commitTime, keyMutations, columnValueStore, key) -> Iterator.of(commitTime.getAdditionTime(times))
                .flatMap(addTime -> Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, addTime)));
        } else {
            this.createMaskedTimestampFunction = txh -> null;
            this.sleepAfterWriteFunction = mustPass -> {};
            this.deletionsFunction = (commitTime, keyMutations, columnValueStore, key) -> Iterator.ofAll(keyMutations.getDeletions())
                .map(deletion -> columnValueStore.deleteColumn(key, deletion));
            this.additionsFunction = (commitTime, keyMutations, columnValueStore, key) -> Iterator.ofAll(keyMutations.getAdditions())
                .map(addition -> columnValueStore.insertColumn(key, addition));
        }
    }

    protected CQLKeyColumnValueStore getColumnValueStore(String tableName){
        CQLKeyColumnValueStore keyColumnValueStore = this.openStores.get(tableName);
        if(keyColumnValueStore == null){
            throw new IllegalStateException("Store cannot be found: " + tableName);
        }
        return keyColumnValueStore;
    }
}
