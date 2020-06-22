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

package org.janusgraph.diskstorage.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.idmanagement.AbstractIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.IOUtils;

/**
 * Corresponding to the id famliy, there is only one cell with each partition
 * row, the column name is HBaseIDStore#ID_COL
 */
public class HBaseIDStore extends HBaseKeyColumnValueStore {

    HBaseIDStore(HBaseStoreManager storeManager, ConnectionMask cnx, String tableName, String columnFamily,
            String storeName) {
        super(storeManager, cnx, tableName, columnFamily, storeName);
    }

    @Override
    protected List<Get> buildGets(List<StaticBuffer> keys, Filter getFilter) throws BackendException {
        Configuration config = this.storeManager.getStorageConfig();
        boolean casUpdate = config.has(GraphDatabaseConfiguration.IDS_CAS)
                ? config.get(GraphDatabaseConfiguration.IDS_CAS)
                : false;
        if (!casUpdate) {
            return super.buildGets(keys, getFilter);
        }
        List<Get> gets = new ArrayList<>(keys.size());
        for (StaticBuffer key : keys) {
            byte[] rowkey = key.as(StaticBuffer.ARRAY_FACTORY);
            Get get = new Get(rowkey);
            get.addColumn(columnFamilyBytes, IDAuthority.ID_COLUMN.as(StaticBuffer.ARRAY_FACTORY));
            gets.add(get);
        }
        return gets;
    }

    @Override
    public boolean casUpdate(StaticBuffer key, Entry entry, StaticBuffer newValue, StoreTransaction txh)
            throws BackendException {
        TableMask table = null;
        boolean res = false;
        try {
            table = cnx.getTable(tableName);
            byte[] rowkey = key.as(StaticBuffer.ARRAY_FACTORY);
            byte[] value = newValue.as(StaticBuffer.ARRAY_FACTORY);
            Put put = new Put(rowkey);
            put.addColumn(columnFamilyBytes, IDAuthority.ID_COLUMN.as(StaticBuffer.ARRAY_FACTORY), value);
            byte[] colValue = entry.getValue().getLong(0) == AbstractIDAuthority.BASE_ID ? null
                    : entry.getValueAs(StaticBuffer.ARRAY_FACTORY);
            res = table.checkAndPut(rowkey, columnFamilyBytes, entry.getColumnAs(StaticBuffer.ARRAY_FACTORY), colValue,
                    put);
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
        return res;
    }
}
