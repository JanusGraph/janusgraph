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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.shaded.datastax.oss.driver.api.core.type.DataTypes;
import org.janusgraph.shaded.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.junit.Test;

import static org.janusgraph.shaded.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.COMPACTION_STRATEGY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CQLKeyColumnValueStoreTest {
    @Test
    public void testCompactionStrategyWithoutOptions() {
        CreateTableWithOptions createTable = createTable("tableName")
            .withPartitionKey("column", DataTypes.BLOB);
        Configuration configuration = buildGraphConfiguration()
            .set(COMPACTION_STRATEGY, "LeveledCompactionStrategy");

        assertDoesNotThrow(() -> {
            CQLKeyColumnValueStore.compactionOptions(createTable, configuration);
        });
    }

    @Test
    public void testCompactionStrategyWithOptions() {
        CreateTableWithOptions createTable = createTable("tableName")
            .withPartitionKey("column", DataTypes.BLOB);
        Configuration configuration = buildGraphConfiguration()
            .set(COMPACTION_OPTIONS, new String[]{"enabled", "false"})
            .set(COMPACTION_STRATEGY, "LeveledCompactionStrategy");

        assertDoesNotThrow(() -> {
            CQLKeyColumnValueStore.compactionOptions(createTable, configuration);
        });
    }
}
