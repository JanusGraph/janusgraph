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

package org.janusgraph.diskstorage.cql.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.cql.function.ConsumerWithBackendException;
import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyLoggedFunction;
import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyUnloggedFunction;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.util.Map;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;

public class CQLMutateManyFunctionBuilder {

    public CQLMutateManyFunction build(final CqlSession session, final Configuration configuration,
                                       final TimestampProvider times, final boolean assignTimestamp,
                                       final Map<String, CQLKeyColumnValueStore> openStores,
                                       ConsumerWithBackendException<DistributedStoreManager.MaskedTimestamp> sleepAfterWriteFunction,
                                       QueryBackPressure queriesBackPressure) {

        int batchSize = configuration.get(BATCH_STATEMENT_SIZE);
        boolean atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);

        final CQLMutateManyFunction mutateManyFunction;
        if (atomicBatch) {
            mutateManyFunction = new CQLMutateManyLoggedFunction(times,
                assignTimestamp, openStores, session, sleepAfterWriteFunction, queriesBackPressure);
        } else {
            mutateManyFunction = new CQLMutateManyUnloggedFunction(batchSize,
                session, openStores, times, assignTimestamp, sleepAfterWriteFunction, queriesBackPressure);
        }

        return mutateManyFunction;
    }

}
