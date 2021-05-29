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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

public class HTable1_0 implements TableMask
{
    private final Table table;

    public HTable1_0(Table table)
    {
        this.table = table;
    }

    @Override
    public ResultScanner getScanner(Scan filter) throws IOException
    {
        return table.getScanner(filter);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException
    {
        return table.get(gets);
    }

    @Override
    public void batch(List<Row> writes, Object[] results) throws IOException, InterruptedException
    {
        table.batch(writes, results);
        /* table.flushCommits(); not needed anymore */
    }

    @Override
    public void close() throws IOException
    {
        table.close();
    }
}
