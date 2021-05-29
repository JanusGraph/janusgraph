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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;

import java.io.IOException;

public interface HBaseCompat {

    /**
     * Configure the compression scheme {@code algorithm} on a column family
     * descriptor {@code cd}. The {@code algorithm} parameter is a string value
     * corresponding to one of the values of HBase's Compression enum. The
     * Compression enum has moved between packages as HBase has evolved, which
     * is why this method has a String argument in the signature instead of the
     * enum itself.
     *
     * @param cd
     *            column family to configure
     * @param algorithm
     *            compression type to use
     */
    void setCompression(HColumnDescriptor cd, String algorithm);

    /**
     * Create and return a HTableDescriptor instance with the given name. The
     * constructors on this method have remained stable over HBase development
     * so far, but the old HTableDescriptor(String) constructor &amp; byte[] friends
     * are now marked deprecated and may eventually be removed in favor of the
     * HTableDescriptor(TableName) constructor. That constructor (and the
     * TableName type) only exists in newer HBase versions. Hence this method.
     *
     * @param tableName
     *            HBase table name
     * @return a new table descriptor instance
     */
    HTableDescriptor newTableDescriptor(String tableName);

    ConnectionMask createConnection(Configuration conf) throws IOException;

    void addColumnFamilyToTableDescriptor(HTableDescriptor tableDescriptor, HColumnDescriptor columnDescriptor);

    void setTimestamp(Delete d, long timestamp);
}
