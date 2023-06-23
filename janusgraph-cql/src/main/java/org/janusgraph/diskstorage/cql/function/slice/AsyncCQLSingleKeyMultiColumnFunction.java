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
import org.janusgraph.diskstorage.cql.query.SingleKeyMultiColumnQuery;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class AsyncCQLSingleKeyMultiColumnFunction extends AsyncCQLFunction<SingleKeyMultiColumnQuery>{
    public AsyncCQLSingleKeyMultiColumnFunction(CqlSession session, PreparedStatement getSlice, CQLColValGetter getter, ExecutorService executorService, QueryBackPressure queryBackPressure) {
        super(session, getSlice, getter, executorService, queryBackPressure);
    }

    @Override
    BoundStatementBuilder bindMarkers(SingleKeyMultiColumnQuery query, BoundStatementBuilder statementBuilder) {
        return statementBuilder.setByteBuffer(CQLKeyColumnValueStore.KEY_BINDING, query.getKey())
            .setList(CQLKeyColumnValueStore.COLUMN_BINDING, query.getColumns(), ByteBuffer.class)
            .setInt(CQLKeyColumnValueStore.LIMIT_BINDING, query.getLimit())
            .setRoutingKey(query.getKey());
    }
}
