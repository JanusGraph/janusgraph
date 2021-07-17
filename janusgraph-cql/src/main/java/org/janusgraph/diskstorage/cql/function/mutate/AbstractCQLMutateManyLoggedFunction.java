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

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;

import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;

public abstract class AbstractCQLMutateManyLoggedFunction extends AbstractCQLMutateManyFunction implements CQLMutateManyFunction {


    public AbstractCQLMutateManyLoggedFunction(TimestampProvider times, boolean assignTimestamp,
                                               Map<String, CQLKeyColumnValueStore> openStores,
                                               ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction) {
        super(sleepAfterWriteFunction, assignTimestamp, times, openStores);
    }

    // Use a single logged batch
    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {

        final DistributedStoreManager.MaskedTimestamp commitTime = createMaskedTimestampFunction.apply(txh);

        BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);
        builder.setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel());

        mutations.forEach((tableName, tableMutations) -> {
            final CQLKeyColumnValueStore columnValueStore = getColumnValueStore(tableName);
            tableMutations.forEach((key, keyMutations) -> {
                deletionsFunction
                    .getBatchableStatementsForColumnOperation(commitTime, keyMutations, columnValueStore, key)
                    .forEach(builder::addStatement);
                additionsFunction
                    .getBatchableStatementsForColumnOperation(commitTime, keyMutations, columnValueStore, key)
                    .forEach(builder::addStatement);
            });
        });

        execute(builder.build());

        sleepAfterWriteFunction.accept(commitTime);

    }

    protected abstract void execute(BatchStatement batchStatement) throws BackendException;

}
