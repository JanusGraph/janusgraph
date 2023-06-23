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
import org.janusgraph.diskstorage.cql.query.MultiKeysSingleSliceQuery;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class AsyncCQLMultiKeySliceFunction extends AsyncCQLFunction<MultiKeysSingleSliceQuery>{
    public AsyncCQLMultiKeySliceFunction(CqlSession session, PreparedStatement getSlice, CQLColValGetter getter, ExecutorService executorService, QueryBackPressure queryBackPressure) {
        super(session, getSlice, getter, executorService, queryBackPressure);
    }

    @Override
    BoundStatementBuilder bindMarkers(MultiKeysSingleSliceQuery query, BoundStatementBuilder statementBuilder) {
        return statementBuilder
            .setList(CQLKeyColumnValueStore.KEY_BINDING, query.getKeys(), ByteBuffer.class)
            .setByteBuffer(CQLKeyColumnValueStore.SLICE_START_BINDING, query.getQuery().getSliceStart().asByteBuffer())
            .setByteBuffer(CQLKeyColumnValueStore.SLICE_END_BINDING, query.getQuery().getSliceEnd().asByteBuffer())
            .setInt(CQLKeyColumnValueStore.LIMIT_BINDING, query.getLimit())
            .setRoutingToken(query.getRoutingToken())
            // usually routing key isn't needed when routingToken is specified,
            // but in some cases different driver implementations (like ScyllaDB)
            // or load balancing strategies may rely on routing key instead.
            // Thus, we also set routing key here to any of the keys because we
            // are sure they all need to be executed on the same node (or shard).
            .setRoutingKey(query.getKeys().get(0));
    }
}
