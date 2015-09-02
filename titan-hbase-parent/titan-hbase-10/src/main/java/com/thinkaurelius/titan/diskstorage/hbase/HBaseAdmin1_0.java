package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseAdmin1_0 implements AdminMask
{

    private static final Logger log = LoggerFactory.getLogger(HBaseAdmin1_0.class);

    private final Admin adm;

    public HBaseAdmin1_0(HBaseAdmin adm)
    {
        this.adm = adm;
    }
    @Override
    public void clearTable(String tableString, long timestamp) throws IOException
    {
        TableName tableName = TableName.valueOf(tableString);

        if (!adm.tableExists(tableName)) {
            log.debug("Attempted to clear table {} before it exists (noop)", tableString);
            return;
        }

        if (!adm.isTableDisabled(tableName))
            adm.disableTable(tableName);

        if (!adm.isTableDisabled(tableName))
            throw new RuntimeException("Unable to disable table " + tableName);

        // This API call appears to both truncate and reenable the table.
        log.info("Truncating table {}", tableName);
        adm.truncateTable(tableName, true /* preserve splits */);

        try {
            adm.enableTable(tableName);
        } catch (TableNotDisabledException e) {
            // This triggers seemingly every time in testing with 1.0.2.
            log.debug("Table automatically reenabled by truncation: {}", tableName, e);
        }
    }

    @Override
    public HTableDescriptor getTableDescriptor(String tableString) throws TableNotFoundException, IOException
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
}
