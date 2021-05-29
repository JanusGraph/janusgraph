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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseAdmin1_0 implements AdminMask
{

    private static final Logger log = LoggerFactory.getLogger(HBaseAdmin1_0.class);

    private final Admin adm;

    public HBaseAdmin1_0(Admin adm)
    {
        this.adm = adm;
    }

    /**
     * Delete all rows from the given table. This method is intended only for development and testing use.
     * @param tableString
     * @param timestamp
     * @throws IOException
     */
    @Override
    public void clearTable(String tableString, long timestamp) throws IOException
    {
        TableName tableName = TableName.valueOf(tableString);

        if (!adm.tableExists(tableName)) {
            log.debug("Attempted to clear table {} before it exists (noop)", tableString);
            return;
        }

        // Unfortunately, linear scanning and deleting rows is faster in HBase when running integration tests than
        // disabling and deleting/truncating tables.
        final Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(2000);
        scan.setTimeRange(0, Long.MAX_VALUE);
        scan.setMaxVersions(1);

        try (final Table table = adm.getConnection().getTable(tableName);
             final ResultScanner scanner = table.getScanner(scan)) {
            final Iterator<Result> iterator = scanner.iterator();
            final int batchSize = 1000;
            final List<Delete> deleteList = new ArrayList<>();
            while (iterator.hasNext()) {
                deleteList.add(new Delete(iterator.next().getRow(), timestamp));
                if (!iterator.hasNext() || deleteList.size() == batchSize) {
                    table.delete(deleteList);
                    deleteList.clear();
                }
            }
        }
    }

    @Override
    public void dropTable(String tableString) throws IOException {
        final TableName tableName = TableName.valueOf(tableString);

        if (!adm.tableExists(tableName)) {
            log.debug("Attempted to drop table {} before it exists (noop)", tableString);
            return;
        }

        if (adm.isTableEnabled(tableName)) {
            adm.disableTable(tableName);
        }
        adm.deleteTable(tableName);
    }

    @Override
    public HTableDescriptor getTableDescriptor(String tableString) throws IOException
    {
        return adm.getTableDescriptor(TableName.valueOf(tableString));
    }

    @Override
    public boolean tableExists(String tableString) throws IOException
    {
        return adm.tableExists(TableName.valueOf(tableString));
    }

    @Override
    public void createTable(HTableDescriptor desc) throws IOException
    {
        adm.createTable(desc);
    }

    @Override
    public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException
    {
        adm.createTable(desc, startKey, endKey, numRegions);
    }

    @Override
    public int getEstimatedRegionServerCount()
    {
        int serverCount = -1;
        try {
            serverCount = adm.getClusterStatus().getServers().size();
            log.debug("Read {} servers from HBase ClusterStatus", serverCount);
        } catch (IOException e) {
            log.debug("Unable to retrieve HBase cluster status", e);
        }
        return serverCount;
    }

    @Override
    public void disableTable(String tableString) throws IOException
    {
        adm.disableTable(TableName.valueOf(tableString));
    }

    @Override
    public void enableTable(String tableString) throws IOException
    {
        adm.enableTable(TableName.valueOf(tableString));
    }

    @Override
    public boolean isTableDisabled(String tableString) throws IOException
    {
        return adm.isTableDisabled(TableName.valueOf(tableString));
    }

    @Override
    public void addColumn(String tableString, HColumnDescriptor columnDescriptor) throws IOException
    {
        adm.addColumn(TableName.valueOf(tableString), columnDescriptor);
    }

    @Override
    public void close() throws IOException
    {
        adm.close();
    }

    @Override
    public void snapshot(String snapshotName, String table) throws IllegalArgumentException, IOException {
        adm.snapshot(snapshotName, TableName.valueOf(table));
    }

    @Override
    public void deleteSnapshot(String snapshotName) throws IOException {
        adm.deleteSnapshot(snapshotName);
    }
}
