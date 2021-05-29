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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This interface hides ABI/API breaking changes that HBase has made to its Table/HTableInterface over the course
 * of development from 0.94 to 1.0 and beyond.
 */
public interface TableMask extends Closeable
{

    ResultScanner getScanner(Scan filter) throws IOException;

    Result[] get(List<Get> gets) throws IOException;

    void batch(List<Row> writes, Object[] results) throws IOException, InterruptedException;

}
