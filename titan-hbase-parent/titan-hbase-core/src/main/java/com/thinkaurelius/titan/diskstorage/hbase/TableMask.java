/**
 * Copyright DataStax, Inc.
 * <p>
 * Please see the included license file for details.
 */
package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;

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
