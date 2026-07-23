// Copyright 2026 JanusGraph Authors
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

import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the storage.cql.cdc option causes the edgestore table DDL to carry the
 * Cassandra cdc=true table option (and only for the edgestore table). The cluster-level
 * effect is validated end-to-end by the full Debezium E2E.
 */
public class CQLCdcTableOptionTest {

    private ModifiableConfiguration config(boolean cdc) {
        ModifiableConfiguration mc = GraphDatabaseConfiguration.buildGraphConfiguration();
        mc.set(CQLConfigOptions.CDC, cdc);
        return mc;
    }

    /** Whether the DDL carries the specific {@code cdc = true} table option (not just any incidental 'cdc' substring). */
    private boolean hasCdcTrue(String cql) {
        return Pattern.compile("cdc\\s*=\\s*true", Pattern.CASE_INSENSITIVE).matcher(cql).find();
    }

    @Test
    public void edgestoreGetsCdcOptionWhenEnabled() {
        String cql = CQLKeyColumnValueStore
            .buildCreateTable("ks", Backend.EDGESTORE_NAME, config(true), 4).asCql();
        assertTrue(hasCdcTrue(cql), cql);
    }

    @Test
    public void edgestoreHasNoCdcOptionWhenDisabled() {
        String cql = CQLKeyColumnValueStore
            .buildCreateTable("ks", Backend.EDGESTORE_NAME, config(false), 4).asCql();
        assertFalse(hasCdcTrue(cql), cql);
    }

    @Test
    public void nonEdgestoreTableHasNoCdcOptionEvenWhenEnabled() {
        String cql = CQLKeyColumnValueStore
            .buildCreateTable("ks", Backend.INDEXSTORE_NAME, config(true), 4).asCql();
        assertFalse(hasCdcTrue(cql), cql);
    }
}
