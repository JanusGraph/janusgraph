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

/**
 * Copyright DataStax, Inc.
 * <p>
 * Please see the included license file for details.
 */
package org.janusgraph.diskstorage.hbase;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.Closeable;
import java.io.IOException;

/**
 * This interface hides ABI/API breaking changes that HBase has made to its Admin/HBaseAdmin over the course
 * of development from 0.94 to 1.0 and beyond.
 */
public interface AdminMask extends Closeable
{

    void clearTable(String tableName, long timestamp) throws IOException;

    /**
     * Drop given table. Table can be either enabled or disabled.
     * @param tableName Name of the table to delete
     * @throws IOException
     */
    void dropTable(String tableName) throws IOException;

    HTableDescriptor getTableDescriptor(String tableName) throws IOException;

    boolean tableExists(String tableName) throws IOException;

    void createTable(HTableDescriptor desc) throws IOException;

    void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException;

    /**
     * Estimate the number of regionservers in the HBase cluster.
     *
     * This is usually implemented by calling
     * {@link HBaseAdmin#getClusterStatus()} and then
     * {@link ClusterStatus#getServers()} and finally {@code size()} on the
     * returned server list.
     *
     * @return the number of servers in the cluster or -1 if it could not be determined
     */
    int getEstimatedRegionServerCount();

    void disableTable(String tableName) throws IOException;

    void enableTable(String tableName) throws IOException;

    boolean isTableDisabled(String tableName) throws IOException;

    void addColumn(String tableName, HColumnDescriptor columnDescriptor) throws IOException;

    void snapshot(String snapshotName, String table) throws IllegalArgumentException, IOException;

    void deleteSnapshot(String snapshotName) throws IOException;
}
