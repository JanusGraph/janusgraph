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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class AsyncCQLSingleKeySliceFunction extends AsyncCQLFunction<KeySliceQuery>{
    public AsyncCQLSingleKeySliceFunction(CqlSession session, PreparedStatement getSlice, CQLColValGetter getter, ExecutorService executorService, QueryBackPressure queryBackPressure) {
        super(session, getSlice, getter, executorService, queryBackPressure);
    }

    @Override
    BoundStatementBuilder bindMarkers(KeySliceQuery query, BoundStatementBuilder statementBuilder) {
        ByteBuffer key = query.getKey().asByteBuffer();
        return statementBuilder.setByteBuffer(CQLKeyColumnValueStore.KEY_BINDING, key)
            .setByteBuffer(CQLKeyColumnValueStore.SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
            .setByteBuffer(CQLKeyColumnValueStore.SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
            .setInt(CQLKeyColumnValueStore.LIMIT_BINDING, query.getLimit())
            .setRoutingKey(key);
    }
}
