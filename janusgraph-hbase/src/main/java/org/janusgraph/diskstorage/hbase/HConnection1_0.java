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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;

public class HConnection1_0 implements ConnectionMask
{

    private final Connection cnx;

    public HConnection1_0(Connection cnx)
    {
        this.cnx = cnx;
    }

    @Override
    public TableMask getTable(String name) throws IOException
    {
        return new HTable1_0(cnx.getTable(TableName.valueOf(name)));
    }

    @Override
    public AdminMask getAdmin() throws IOException
    {
        return new HBaseAdmin1_0(cnx.getAdmin());
    }

    @Override
    public void close() throws IOException
    {
        cnx.close();
    }

    @Override
    public List<HRegionLocation> getRegionLocations(String tableName)
        throws IOException
    {
        return this.cnx.getRegionLocator(TableName.valueOf(tableName)).getAllRegionLocations();
    }
}
