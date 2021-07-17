// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.cql.Row;
import io.vavr.Tuple3;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry.GetColVal;

public class CQLColValGetter implements GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> {

    private final EntryMetaData[] schema;

    CQLColValGetter(final EntryMetaData[] schema) {
        this.schema = schema;
    }

    @Override
    public StaticBuffer getColumn(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return tuple._1;
    }

    @Override
    public StaticBuffer getValue(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return tuple._2;
    }

    @Override
    public EntryMetaData[] getMetaSchema(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return this.schema;
    }

    @Override
    public Object getMetaData(final Tuple3<StaticBuffer, StaticBuffer, Row> tuple, final EntryMetaData metaData) {
        switch (metaData) {
            case TIMESTAMP:
                return tuple._3.getLong(CQLKeyColumnValueStore.WRITETIME_COLUMN_NAME);
            case TTL:
                return tuple._3.getInt(CQLKeyColumnValueStore.TTL_COLUMN_NAME);
            default:
                throw new UnsupportedOperationException("Unsupported meta data: " + metaData);
        }
    }
}
