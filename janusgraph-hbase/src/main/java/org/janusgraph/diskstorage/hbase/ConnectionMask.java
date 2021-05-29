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

import org.apache.hadoop.hbase.HRegionLocation;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This interface hides ABI/API breaking changes that HBase has made to its (H)Connection class over the course
 * of development from 0.94 to 1.0 and beyond.
 */
public interface ConnectionMask extends Closeable
{

    /**
     * Retrieve the TableMask compatibility layer object for the supplied table name.
     * @param name
     * @return The TableMask for the specified table.
     * @throws IOException in the case of backend exceptions.
     */
    TableMask getTable(String name) throws IOException;

    /**
     * Retrieve the AdminMask compatibility layer object for this Connection.
     * @return The AdminMask for this Connection
     * @throws IOException in the case of backend exceptions.
     */
    AdminMask getAdmin() throws IOException;

    /**
     * Retrieve the RegionLocations for the supplied table name.
     * @param tableName
     * @return A map of HRegionInfo to ServerName that describes the storage regions for the named table.
     * @throws IOException in the case of backend exceptions.
     */
    List<HRegionLocation> getRegionLocations(String tableName) throws IOException;
}
