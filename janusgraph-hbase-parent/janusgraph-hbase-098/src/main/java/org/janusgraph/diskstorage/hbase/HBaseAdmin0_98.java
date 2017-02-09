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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.util.system.IOUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseAdmin0_98 implements AdminMask
{

    private static final Logger log = LoggerFactory.getLogger(HBaseAdmin0_98.class);

    private final HBaseAdmin adm;

    public HBaseAdmin0_98(HBaseAdmin adm)
    {
        this.adm = adm;
    }

    @Override
    public void clearTable(String tableName, long timestamp) throws IOException
    {
        if (!adm.tableExists(tableName)) {
            log.debug("clearStorage() called before table {} was created, skipping.", tableName);
            return;
        }

//        long before = System.currentTimeMillis();
//        try {
//            adm.disableTable(tableName);
//            adm.deleteTable(tableName);
//        } catch (IOException e) {
//            throw new PermanentBackendException(e);
//        }
//        ensureTableExists(tableName, getCfNameForStoreName(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME), 0);
//        long after = System.currentTimeMillis();
//        logger.debug("Dropped and recreated table {} in {} ms", tableName, after - before);


        // Unfortunately, linear scanning and deleting tables is faster in HBase < 1 when running integration tests than
        // disabling and deleting tables.
        HTable table = null;

        try {
            table = new HTable(adm.getConfiguration(), tableName);

            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setCacheBlocks(false);
            scan.setCaching(2000);
            scan.setTimeRange(0, Long.MAX_VALUE);
            scan.setMaxVersions(1);

            ResultScanner scanner = null;

            try {
                scanner = table.getScanner(scan);

                for (Result res : scanner) {
                    Delete d = new Delete(res.getRow());

                    d.setTimestamp(timestamp);
                    table.delete(d);
                }
            } finally {
                IOUtils.closeQuietly(scanner);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public HTableDescriptor getTableDescriptor(String tableName) throws TableNotFoundException, IOException
    {
        return adm.getTableDescriptor(tableName.getBytes());
    }

    @Override
    public boolean tableExists(String tableName) throws IOException
    {
        return adm.tableExists(tableName);
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
    public void disableTable(String tableName) throws IOException
    {
        adm.disableTable(tableName);
    }

    @Override
    public void enableTable(String tableName) throws IOException
    {
        adm.enableTable(tableName);
    }

    @Override
    public boolean isTableDisabled(String tableName) throws IOException
    {
        return adm.isTableDisabled(tableName);
    }

    @Override
    public void addColumn(String tableName, HColumnDescriptor columnDescriptor) throws IOException
    {
        adm.addColumn(tableName, columnDescriptor);
    }

    @Override
    public void close() throws IOException
    {
        adm.close();
    }
}
