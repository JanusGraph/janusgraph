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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

public class HBaseCompat1_0 implements HBaseCompat {

    @Override
    public void setCompression(HColumnDescriptor cd, String algorithm) {
        cd.setCompressionType(Compression.Algorithm.valueOf(algorithm));
    }

    @Override
    public HTableDescriptor newTableDescriptor(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        return new HTableDescriptor(tn);
    }

    @Override
    public ConnectionMask createConnection(Configuration conf) throws IOException
    {
        return new HConnection1_0(ConnectionFactory.createConnection(conf));
    }

    @Override
    public void addColumnFamilyToTableDescriptor(HTableDescriptor tableDescriptor, HColumnDescriptor columnDescriptor)
    {
        tableDescriptor.addFamily(columnDescriptor);
    }

    @Override
    public void setTimestamp(Delete d, long timestamp)
    {
        d.setTimestamp(timestamp);
    }

}
