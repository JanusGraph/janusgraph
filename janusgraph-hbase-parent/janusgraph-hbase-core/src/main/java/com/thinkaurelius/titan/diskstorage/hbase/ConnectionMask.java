/**
 * Copyright DataStax, Inc.
 * <p>
 * Please see the included license file for details.
 */
package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.Closeable;
import java.io.IOException;

/**
 * This interface hides ABI/API breaking changes that HBase has made to its (H)Connection class over the course
 * of development from 0.94 to 1.0 and beyond.
 */
public interface ConnectionMask extends Closeable
{

    TableMask getTable(String name) throws IOException;

    AdminMask getAdmin() throws IOException;
}
