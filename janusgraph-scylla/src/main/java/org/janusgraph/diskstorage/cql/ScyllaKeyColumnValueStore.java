// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.configuration.Configuration;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;

public class ScyllaKeyColumnValueStore extends CQLKeyColumnValueStore {
    /**
     * Creates an instance of the {@link ScyllaKeyColumnValueStore} that stores the data in a CQL backed table.
     *
     * @param storeManager  the {@link CQLStoreManager} that maintains the list of {@link CQLKeyColumnValueStore}s
     * @param tableName     the name of the database table for storing the key/column/values
     * @param configuration data used in creating this store
     * @param closer        callback used to clean up references to this store in the store manager
     */
    public ScyllaKeyColumnValueStore(CQLStoreManager storeManager, String tableName, Configuration configuration, Runnable closer) {
        super(storeManager, tableName, configuration, closer);
    }

    @Override
    protected CreateTableWithOptions compressionOptions(final CreateTableWithOptions createTable,
                                                        final Configuration configuration) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return createTable.withNoCompression();
        }

        String compressionType = configuration.get(CF_COMPRESSION_TYPE);
        int chunkLengthInKb = configuration.get(CF_COMPRESSION_BLOCK_SIZE);

        return createTable.withOption("compression",
            ImmutableMap.of("sstable_compression", compressionType, "chunk_length_kb", chunkLengthInKb));
    }
}
