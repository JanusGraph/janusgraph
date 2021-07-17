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

package org.janusgraph.diskstorage.cql.function;

import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import io.vavr.collection.Iterator;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;

public interface ColumnOperationFunction {

    Iterator<BatchableStatement<BoundStatement>> getBatchableStatementsForColumnOperation(final DistributedStoreManager.MaskedTimestamp commitTime,
                                                                                          final KCVMutation keyMutations,
                                                                                          final CQLKeyColumnValueStore columnValueStore,
                                                                                          final StaticBuffer key);

}
