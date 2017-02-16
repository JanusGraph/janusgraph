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
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

public class HConnection0_98 implements ConnectionMask
{

    private final HConnection cnx;

    public HConnection0_98(HConnection cnx)
    {
        this.cnx = cnx;
    }

    @Override
    public TableMask getTable(String name) throws IOException
    {
        return new HTable0_98(cnx.getTable(name));
    }

    @Override
    public AdminMask getAdmin() throws IOException
    {
        return new HBaseAdmin0_98(new HBaseAdmin(cnx));
    }

    @Override
    public void close() throws IOException
    {
        cnx.close();
    }

    @Override
    public List<HRegionLocation> getRegionLocations(String tableName) throws IOException
    {
        HTable table = null;
        try {
            table = new HTable(cnx.getConfiguration(), tableName);
            return table.getRegionLocations().entrySet().stream()
                .map(e -> { return new HRegionLocation(e.getKey(), e.getValue()); })
                .collect(Collectors.toList());
        } finally {
            IOUtils.closeQuietly(table);
        }
    }
}
