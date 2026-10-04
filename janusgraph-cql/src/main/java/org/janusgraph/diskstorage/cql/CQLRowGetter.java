// Copyright 2026 JanusGraph Authors
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

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry.GetColVal;

import java.nio.ByteBuffer;

/**
 * Reads the column, the value and the meta data of a row of a slice query by the indexes of the row's columns, which it
 * resolves once from the definitions of the result, where a lookup by name costs every cell a lower-cased hash lookup
 * and two allocations. The buffers it hands out are the driver's own ({@link Row#getBytesUnsafe(int)}): they are
 * read from their position on and never consumed, so a row can be read more than once, as sizing a result before
 * copying it does.
 */
public class CQLRowGetter implements GetColVal<Row, ByteBuffer> {

    private final EntryMetaData[] schema;
    private final int columnIndex;
    private final int valueIndex;
    private final int writetimeIndex;
    private final int ttlIndex;
    private final int keyIndex;

    /**
     * @param schema  the meta data to read with each entry; each kind needs its column in the result
     * @param columns the columns of the result of the slice query
     */
    public CQLRowGetter(final EntryMetaData[] schema, final ColumnDefinitions columns) {
        this.schema = schema;
        this.columnIndex = indexOf(columns, CQLKeyColumnValueStore.COLUMN_COLUMN_NAME);
        this.valueIndex = indexOf(columns, CQLKeyColumnValueStore.VALUE_COLUMN_NAME);
        int writetimeIndex = -1;
        int ttlIndex = -1;
        int keyIndex = -1;
        for (EntryMetaData metaData : schema) {
            switch (metaData) {
                case TIMESTAMP:
                    writetimeIndex = indexOf(columns, CQLKeyColumnValueStore.WRITETIME_COLUMN_NAME);
                    break;
                case TTL:
                    ttlIndex = indexOf(columns, CQLKeyColumnValueStore.TTL_COLUMN_NAME);
                    break;
                case ROW_KEY:
                    keyIndex = indexOf(columns, CQLKeyColumnValueStore.KEY_COLUMN_NAME);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + metaData);
            }
        }
        this.writetimeIndex = writetimeIndex;
        this.ttlIndex = ttlIndex;
        this.keyIndex = keyIndex;
    }

    private static int indexOf(final ColumnDefinitions columns, final String name) {
        final int index = columns.firstIndexOf(name);
        Preconditions.checkArgument(index >= 0, "The result of the slice query has no column %s", name);
        return index;
    }

    @Override
    public ByteBuffer getColumn(final Row row) {
        return row.getBytesUnsafe(columnIndex);
    }

    @Override
    public ByteBuffer getValue(final Row row) {
        return row.getBytesUnsafe(valueIndex);
    }

    @Override
    public EntryMetaData[] getMetaSchema(final Row row) {
        return schema;
    }

    @Override
    public Object getMetaData(final Row row, final EntryMetaData metaData) {
        switch (metaData) {
            case TIMESTAMP:
                return row.getLong(writetimeIndex);
            case TTL:
                return row.getInt(ttlIndex);
            case ROW_KEY:
                final ByteBuffer rawKey = row.getBytesUnsafe(keyIndex);
                return rawKey == null ? CQLColValGetter.EMPTY_KEY : StaticArrayBuffer.of(rawKey);
            default:
                throw new UnsupportedOperationException("Unsupported meta data: " + metaData);
        }
    }
}
